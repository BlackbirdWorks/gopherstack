package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

type instanceTypeOfferingItem struct {
	InstanceType string `xml:"instanceType"`
	Location     string `xml:"location"`
	LocationType string `xml:"locationType"`
}

type keyPairItem struct {
	KeyPairID      string          `xml:"keyPairId"`
	KeyName        string          `xml:"keyName"`
	KeyFingerprint string          `xml:"keyFingerprint"`
	KeyType        string          `xml:"keyType"`
	CreateTime     time.Time       `xml:"createTime"`
	PublicKey      string          `xml:"publicKey,omitempty"`
	TagSet         []simpleTagItem `xml:"tagSet>item"`
}

type keyPairItemSet struct {
	Items []keyPairItem `xml:"item"`
}

type describeKeyPairsResponse struct {
	XMLName   xml.Name       `xml:"DescribeKeyPairsResponse"`
	Xmlns     string         `xml:"xmlns,attr"`
	RequestID string         `xml:"requestId"`
	KeySet    keyPairItemSet `xml:"keySet"`
}

type createKeyPairResponse struct {
	XMLName        xml.Name        `xml:"CreateKeyPairResponse"`
	Xmlns          string          `xml:"xmlns,attr"`
	RequestID      string          `xml:"requestId"`
	KeyPairID      string          `xml:"keyPairId"`
	KeyName        string          `xml:"keyName"`
	KeyFingerprint string          `xml:"keyFingerprint"`
	KeyMaterial    string          `xml:"keyMaterial,omitempty"`
	TagSet         []simpleTagItem `xml:"tagSet>item"`
}

type deleteKeyPairResponse struct {
	XMLName   xml.Name `xml:"DeleteKeyPairResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleCreateKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")
	tags := parseTagSpecification(vals, "key-pair")

	kp, err := h.Backend.CreateKeyPair(name, tags)
	if err != nil {
		return nil, err
	}

	return &createKeyPairResponse{
		Xmlns:          ec2XMLNS,
		RequestID:      reqID,
		KeyPairID:      kp.KeyPairID,
		KeyName:        kp.Name,
		KeyFingerprint: kp.Fingerprint,
		KeyMaterial:    kp.Material,
		TagSet:         tagItemsFromMap(h.Backend.TagsForResource(kp.Name)),
	}, nil
}

func (h *Handler) handleDescribeKeyPairs(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "KeyName")
	kps := h.Backend.DescribeKeyPairs(names)

	filters := parseEC2Filters(vals)
	kps = applyKeyPairFilters(kps, filters, h.Backend)

	includePublicKey, _ := strconv.ParseBool(vals.Get("IncludePublicKey"))

	items := make([]keyPairItem, 0, len(kps))
	for _, kp := range kps {
		item := keyPairItem{
			KeyPairID:      kp.KeyPairID,
			KeyName:        kp.Name,
			KeyFingerprint: kp.Fingerprint,
			KeyType:        kp.KeyType,
			CreateTime:     kp.CreateTime,
			TagSet:         tagItemsFromMap(h.Backend.TagsForResource(kp.Name)),
		}

		if includePublicKey {
			item.PublicKey = kp.PublicKey
		}

		items = append(items, item)
	}

	return &describeKeyPairsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		KeySet:    keyPairItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDeleteKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteKeyPair(name); err != nil {
		return nil, err
	}

	return &deleteKeyPairResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleImportKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")

	if vals.Get("PublicKeyMaterial") == "" {
		return nil, fmt.Errorf("%w: PublicKeyMaterial is required", ErrInvalidParameter)
	}

	tags := parseTagSpecification(vals, "key-pair")

	kp, err := h.Backend.ImportKeyPair(name, vals.Get("PublicKeyMaterial"), tags)
	if err != nil {
		return nil, err
	}

	return &createKeyPairResponse{
		Xmlns:          ec2XMLNS,
		RequestID:      reqID,
		KeyPairID:      kp.KeyPairID,
		KeyName:        kp.Name,
		KeyFingerprint: kp.Fingerprint,
		TagSet:         tagItemsFromMap(h.Backend.TagsForResource(kp.Name)),
	}, nil
}
