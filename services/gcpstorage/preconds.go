package gcpstorage

// Preconds are the ifGeneration*/ifMetageneration* request conditions.
type Preconds struct {
	GenMatch     *int64
	GenNotMatch  *int64
	MetaMatch    *int64
	MetaNotMatch *int64
}

func notMatchErr(read bool) *APIError {
	if read {
		return errNotModified()
	}

	return errPrecondition()
}

// checkObject evaluates the conditions against o (nil when absent). A failed
// not-match condition on a read yields 304 instead of 412.
func (p Preconds) checkObject(o *Object, read bool) *APIError {
	if p.mustMatchFails(o) {
		return errPrecondition()
	}

	if p.notMatchFails(o) {
		return notMatchErr(read)
	}

	return nil
}

func (p Preconds) mustMatchFails(o *Object) bool {
	exists := o != nil

	if v := p.GenMatch; v != nil {
		if (*v == 0 && exists) || (*v != 0 && (!exists || o.Generation != *v)) {
			return true
		}
	}

	v := p.MetaMatch

	return v != nil && (!exists || o.Metageneration != *v)
}

func (p Preconds) notMatchFails(o *Object) bool {
	exists := o != nil

	if v := p.GenNotMatch; v != nil {
		if (*v == 0 && !exists) || (exists && o.Generation == *v) {
			return true
		}
	}

	v := p.MetaNotMatch

	return v != nil && exists && o.Metageneration == *v
}

func (p Preconds) checkBucket(b *Bucket) *APIError {
	if v := p.MetaMatch; v != nil && b.Metageneration != *v {
		return errPrecondition()
	}

	if v := p.MetaNotMatch; v != nil && b.Metageneration == *v {
		return errPrecondition()
	}

	return nil
}
