package ociartifacts

import (
	"net/http"
	"strconv"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateContainerRepo:   (*Handler).createContainerRepo,
		opGetContainerRepo:      (*Handler).getContainerRepo,
		opListContainerRepos:    (*Handler).listContainerRepos,
		opUpdateContainerRepo:   (*Handler).updateContainerRepo,
		opDeleteContainerRepo:   (*Handler).deleteContainerRepo,
		opChangeContainerRepoC:  (*Handler).changeContainerRepoCompartment,
		opListContainerImages:   (*Handler).listImages,
		opGetContainerImage:     (*Handler).getImage,
		opDeleteContainerImage:  (*Handler).deleteImage,
		opUpdateContainerImage:  (*Handler).updateImage,
		opRestoreContainerImage: (*Handler).restoreImage,
		opRemoveContainerVer:    (*Handler).removeVersion,
		opLookupImageByURI:      (*Handler).lookupImage,
		opCreateImageSignature:  (*Handler).createSignature,
		opGetImageSignature:     (*Handler).getSignature,
		opListImageSignatures:   (*Handler).listSignatures,
		opUpdateImageSignature:  (*Handler).updateSignature,
		opDeleteImageSignature:  (*Handler).deleteSignature,
		opGetContainerConfig:    (*Handler).getConfig,
		opUpdateContainerConfig: (*Handler).updateConfig,
		opCreateRepository:      (*Handler).createGenericRepo,
		opGetRepository:         (*Handler).getGenericRepo,
		opListRepositories:      (*Handler).listGenericRepos,
		opUpdateRepository:      (*Handler).updateGenericRepo,
		opDeleteRepository:      (*Handler).deleteGenericRepo,
		opChangeRepositoryComp:  (*Handler).changeGenericRepoCompartment,
		opGetGenericArtifact:    (*Handler).getArtifact,
		opGetGenericByPath:      (*Handler).getArtifactByPath,
		opListGenericArtifacts:  (*Handler).listArtifacts,
		opUpdateGenericArtifact: (*Handler).updateArtifact,
		opUpdateGenericByPath:   (*Handler).updateArtifactByPath,
		opDeleteGenericArtifact: (*Handler).deleteArtifact,
		opDeleteGenericByPath:   (*Handler).deleteArtifactByPath,
		opPutGenericContent:     (*Handler).putContent,
		opGetGenericContent:     (*Handler).getContent,
		opGetGenericContentPath: (*Handler).getContentByPath,
	}
}

// respond writes a 200 JSON body with the etag, or the error.
func respond[T any](q *req, out T, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, out, etag)
}

// withBody decodes the request body into In, then runs call.
func withBody[In any](q *req, call func(in In) error) error {
	var in In
	if err := q.decode(&in); err != nil {
		return err
	}

	return call(in)
}

func noContent(q *req, err error) error {
	if err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func okEmpty(q *req, err error) error {
	if err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusOK)

	return nil
}

func (h *Handler) createContainerRepo(q *req) error {
	return withBody(q, func(in createRepoIn) error {
		w, etag, err := h.Backend.CreateContainerRepository(in, q.user())

		return respond(q, w, etag, err)
	})
}

func (h *Handler) getContainerRepo(q *req) error {
	w, etag, err := h.Backend.GetContainerRepository(q.param("repositoryId"))

	return respond(q, w, etag, err)
}

func (h *Handler) listContainerRepos(q *req) error {
	f := repoFilter{
		compartment: q.query("compartmentId"), id: q.query("repositoryId"), name: q.query("displayName"),
		state: q.query("lifecycleState"), isPublic: q.boolQuery("isPublic"),
	}

	out, next, err := h.Backend.ListContainerRepositories(f, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(next)

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) updateContainerRepo(q *req) error {
	return withBody(q, func(in updateRepoIn) error {
		w, etag, err := h.Backend.UpdateContainerRepository(q.param("repositoryId"), q.ifMatch(), in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) deleteContainerRepo(q *req) error {
	return noContent(q, h.Backend.DeleteContainerRepository(q.param("repositoryId"), q.ifMatch()))
}

func (h *Handler) changeContainerRepoCompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) error {
		return okEmpty(q, h.Backend.ChangeContainerRepositoryCompartment(q.param("repositoryId"), q.ifMatch(), in))
	})
}

func (h *Handler) listImages(q *req) error {
	f := imageFilter{
		isVersioned: q.boolQuery("isVersioned"), compartment: q.query("compartmentId"), name: q.query("displayName"),
		id: q.query("imageId"), repoID: q.query("repositoryId"), repoName: q.query("repositoryName"),
		version: q.query("version"), state: q.query("lifecycleState"), digest: q.query("imageDigest"),
	}

	out, next, err := h.Backend.ListContainerImages(f, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(next)

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) getImage(q *req) error {
	w, etag, err := h.Backend.GetContainerImage(q.param("imageId"))

	return respond(q, w, etag, err)
}

func (h *Handler) deleteImage(q *req) error {
	return noContent(q, h.Backend.DeleteContainerImage(q.param("imageId"), q.ifMatch()))
}

func (h *Handler) updateImage(q *req) error {
	return withBody(q, func(in updateTagsIn) error {
		w, etag, err := h.Backend.UpdateContainerImage(q.param("imageId"), q.ifMatch(), in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) restoreImage(q *req) error {
	var in restoreImageIn

	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	w, etag, err := h.Backend.RestoreContainerImage(q.param("imageId"), q.ifMatch(), in)

	return respond(q, w, etag, err)
}

func (h *Handler) removeVersion(q *req) error {
	return withBody(q, func(in removeVersionIn) error {
		w, etag, err := h.Backend.RemoveContainerVersion(q.param("imageId"), q.ifMatch(), in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) lookupImage(q *req) error {
	return withBody(q, func(in lookupIn) error {
		w, etag, err := h.Backend.LookupContainerImageByURI(in.ImageURI)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) createSignature(q *req) error {
	return withBody(q, func(in createSignatureIn) error {
		w, etag, err := h.Backend.CreateContainerImageSignature(in, q.user())

		return respond(q, w, etag, err)
	})
}

func (h *Handler) getSignature(q *req) error {
	w, etag, err := h.Backend.GetContainerImageSignature(q.param("imageSignatureId"))

	return respond(q, w, etag, err)
}

func (h *Handler) listSignatures(q *req) error {
	f := signatureFilter{
		compartment: q.query("compartmentId"), imageID: q.query("imageId"), repoID: q.query("repositoryId"),
		repoName: q.query("repositoryName"), digest: q.query("imageDigest"), name: q.query("displayName"),
		kmsKeyID: q.query("kmsKeyId"), kmsVersion: q.query("kmsKeyVersionId"), algorithm: q.query("signingAlgorithm"),
	}

	out, next, err := h.Backend.ListContainerImageSignatures(f, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(next)

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) updateSignature(q *req) error {
	return withBody(q, func(in updateTagsIn) error {
		w, etag, err := h.Backend.UpdateContainerImageSignature(q.param("imageSignatureId"), q.ifMatch(), in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) deleteSignature(q *req) error {
	return noContent(q, h.Backend.DeleteContainerImageSignature(q.param("imageSignatureId"), q.ifMatch()))
}

func (h *Handler) getConfig(q *req) error {
	w, etag, err := h.Backend.GetContainerConfiguration(q.query("compartmentId"))

	return respond(q, w, etag, err)
}

func (h *Handler) updateConfig(q *req) error {
	return withBody(q, func(in updateConfigIn) error {
		w, etag, err := h.Backend.UpdateContainerConfiguration(q.query("compartmentId"), q.ifMatch(), in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) createGenericRepo(q *req) error {
	return withBody(q, func(in createGenericRepoIn) error {
		w, etag, err := h.Backend.CreateRepository(in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) getGenericRepo(q *req) error {
	w, etag, err := h.Backend.GetRepository(q.param("repositoryId"))

	return respond(q, w, etag, err)
}

func (h *Handler) listGenericRepos(q *req) error {
	f := genericRepoFilter{
		isImmutable: q.boolQuery("isImmutable"), compartment: q.query("compartmentId"), id: q.query("id"),
		name: q.query("displayName"), state: q.query("lifecycleState"),
	}

	out, next, err := h.Backend.ListRepositories(f, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(next)

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) updateGenericRepo(q *req) error {
	return withBody(q, func(in updateGenericRepoIn) error {
		w, etag, err := h.Backend.UpdateRepository(q.param("repositoryId"), q.ifMatch(), in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) deleteGenericRepo(q *req) error {
	return noContent(q, h.Backend.DeleteRepository(q.param("repositoryId"), q.ifMatch()))
}

func (h *Handler) changeGenericRepoCompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) error {
		return okEmpty(q, h.Backend.ChangeRepositoryCompartment(q.param("repositoryId"), q.ifMatch(), in))
	})
}

func (h *Handler) getArtifact(q *req) error {
	w, etag, err := h.Backend.GetGenericArtifact(q.param("artifactId"))

	return respond(q, w, etag, err)
}

func (h *Handler) getArtifactByPath(q *req) error {
	w, etag, err := h.Backend.GetGenericArtifactByPath(
		q.param("repositoryId"),
		q.param("artifactPath"),
		q.param("version"),
	)

	return respond(q, w, etag, err)
}

func (h *Handler) listArtifacts(q *req) error {
	f := genericArtifactFilter{
		compartment: q.query("compartmentId"), repoID: q.query("repositoryId"), id: q.query("id"),
		name: q.query("displayName"), path: q.query("artifactPath"), version: q.query("version"),
		sha256: q.query("sha256"), state: q.query("lifecycleState"),
	}

	out, next, err := h.Backend.ListGenericArtifacts(f, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(next)

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) updateArtifact(q *req) error {
	return withBody(q, func(in updateTagsIn) error {
		w, etag, err := h.Backend.UpdateGenericArtifact(q.param("artifactId"), q.ifMatch(), in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) updateArtifactByPath(q *req) error {
	return withBody(q, func(in updateTagsIn) error {
		w, etag, err := h.Backend.UpdateGenericArtifactByPath(
			q.param("repositoryId"), q.param("artifactPath"), q.param("version"), q.ifMatch(), in)

		return respond(q, w, etag, err)
	})
}

func (h *Handler) deleteArtifact(q *req) error {
	return noContent(q, h.Backend.DeleteGenericArtifact(q.param("artifactId"), q.ifMatch()))
}

func (h *Handler) deleteArtifactByPath(q *req) error {
	return noContent(q, h.Backend.DeleteGenericArtifactByPath(
		q.param("repositoryId"), q.param("artifactPath"), q.param("version"), q.ifMatch()))
}

func (h *Handler) putContent(q *req) error {
	w, etag, err := h.Backend.PutGenericArtifactContentByPath(
		q.param("repositoryId"), q.param("artifactPath"), q.param("version"), q.ifMatch(), q.body)

	return respond(q, w, etag, err)
}

func (q *req) content(c GenericContent) error {
	q.header("etag", c.Etag)
	q.header("Content-Type", "application/octet-stream")
	q.header("Content-Length", strconv.Itoa(len(c.Content)))
	q.w.WriteHeader(http.StatusOK)
	_, _ = q.w.Write(c.Content)

	return nil
}

func (h *Handler) getContent(q *req) error {
	c, err := h.Backend.GetGenericArtifactContent(q.param("artifactId"))
	if err != nil {
		return err
	}

	return q.content(c)
}

func (h *Handler) getContentByPath(q *req) error {
	c, err := h.Backend.GetGenericArtifactContentByPath(
		q.param("repositoryId"),
		q.param("artifactPath"),
		q.param("version"),
	)
	if err != nil {
		return err
	}

	return q.content(c)
}
