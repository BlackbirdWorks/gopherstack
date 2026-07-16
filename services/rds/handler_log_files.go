package rds

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

func (h *Handler) handleDescribeDBLogFiles(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	filter := LogFileFilter{FilenameContains: vals.Get("FilenameContains")}
	if v := vals.Get("FileLastWritten"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.FileLastWritten = n
		}
	}
	if v := vals.Get("FileSize"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.FileSize = n
		}
	}
	files, err := h.Backend.DescribeDBLogFiles(instanceID, filter)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBLogFile, 0, len(files))
	for _, f := range files {
		members = append(members, xmlDBLogFile(f))
	}

	return &describeDBLogFilesResponse{
		Xmlns:              rdsXMLNS,
		DescribeDBLogFiles: xmlDBLogFileList{Members: members},
	}, nil
}

func (h *Handler) handleDownloadDBLogFilePortion(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	logFileName := vals.Get("LogFileName")
	marker := vals.Get("Marker")
	numberOfLines := 0
	if v := vals.Get("NumberOfLines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			numberOfLines = n
		}
	}
	portion, err := h.Backend.DownloadDBLogFilePortion(instanceID, logFileName, marker, numberOfLines)
	if err != nil {
		return nil, err
	}

	return &downloadDBLogFilePortionResponse{
		Xmlns:                 rdsXMLNS,
		LogFileData:           portion.LogFileData,
		AdditionalDataPending: portion.AdditionalDataPending,
		Marker:                portion.Marker,
	}, nil
}

type xmlDBLogFile struct {
	LogFileName string `xml:"LogFileName"`
	LastWritten int64  `xml:"LastWritten"`
	Size        int64  `xml:"Size"`
}

type xmlDBLogFileList struct {
	Members []xmlDBLogFile `xml:"DescribeDBLogFilesDetails"`
}

type describeDBLogFilesResponse struct {
	XMLName            xml.Name         `xml:"DescribeDBLogFilesResponse"`
	Xmlns              string           `xml:"xmlns,attr"`
	DescribeDBLogFiles xmlDBLogFileList `xml:"DescribeDBLogFilesResult>DescribeDBLogFiles"`
}

type downloadDBLogFilePortionResponse struct {
	XMLName               xml.Name `xml:"DownloadDBLogFilePortionResponse"`
	Xmlns                 string   `xml:"xmlns,attr"`
	LogFileData           string   `xml:"DownloadDBLogFilePortionResult>LogFileData"`
	Marker                string   `xml:"DownloadDBLogFilePortionResult>Marker,omitempty"`
	AdditionalDataPending bool     `xml:"DownloadDBLogFilePortionResult>AdditionalDataPending"`
}
