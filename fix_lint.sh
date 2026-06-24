#!/bin/bash
sed -i 's/func (h \*Handler) deleteTagsForResource(resourceID string) {/func (h \*Handler) deleteTagsForResource(_ string) {/' services/route53/handler.go
sed -i 's/h.Backend.ChangeTagsForResource(resourceID, kv, nil)/_ = h.Backend.ChangeTagsForResource(resourceID, kv, nil)/' services/route53/handler.go
sed -i 's/h.Backend.ChangeTagsForResource(resourceID, nil, keys)/_ = h.Backend.ChangeTagsForResource(resourceID, nil, keys)/' services/route53/handler.go
