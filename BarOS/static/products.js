const uploadImageUrl = window.BAROS_UPLOAD_IMAGE_URL || "";

function initializeImageUploadField(container) {
  const urlInput = container.querySelector("[data-image-url-input]");
  const fileInput = container.querySelector(".image-upload-input");
  const triggerButton = container.querySelector(".image-upload-trigger");
  const statusNode = container.querySelector(".image-upload-status");
  const previewImage = container.querySelector(".image-upload-preview-image");
  const previewPlaceholder = container.querySelector(".image-upload-placeholder");
  const previewWrapper = container.querySelector(".image-upload-preview");

  if (!urlInput || !fileInput || !triggerButton || !statusNode || !previewImage || !previewPlaceholder || !previewWrapper) {
    return;
  }

  let localPreviewUrl = null;

  const setStatus = (message, tone = "") => {
    statusNode.textContent = message || "";
    statusNode.dataset.tone = tone;
  };

  const releaseLocalPreview = () => {
    if (localPreviewUrl) {
      URL.revokeObjectURL(localPreviewUrl);
      localPreviewUrl = null;
    }
  };

  const setPreview = (source) => {
    const hasSource = Boolean(source && source.trim());
    previewWrapper.classList.toggle("has-image", hasSource);
    previewImage.classList.toggle("hidden", !hasSource);
    previewPlaceholder.classList.toggle("hidden", hasSource);

    if (hasSource) {
      previewImage.src = source;
      return;
    }

    previewImage.removeAttribute("src");
  };

  urlInput.addEventListener("input", () => {
    releaseLocalPreview();
    setPreview(urlInput.value);
    if (!urlInput.value.trim()) {
      setStatus("");
    }
  });

  triggerButton.addEventListener("click", () => {
    fileInput.click();
  });

  fileInput.addEventListener("change", async () => {
    const selectedFile = fileInput.files && fileInput.files[0];
    if (!selectedFile) {
      return;
    }

    releaseLocalPreview();
    localPreviewUrl = URL.createObjectURL(selectedFile);
    setPreview(localPreviewUrl);
    setStatus("Enviando imagem...", "info");
    triggerButton.disabled = true;

    const formData = new FormData();
    formData.append("image", selectedFile);

    try {
      const response = await fetch(uploadImageUrl, {
        method: "POST",
        body: formData,
        headers: {
          "X-Requested-With": "XMLHttpRequest",
        },
      });
      const payload = await response.json().catch(() => ({}));

      if (!response.ok || !payload.url) {
        throw new Error(payload.error || "Falha ao enviar a imagem.");
      }

      releaseLocalPreview();
      urlInput.value = payload.url;
      setPreview(payload.url);
      setStatus("Imagem enviada e URL preenchida.", "success");
    } catch (error) {
      setStatus(error.message || "Falha ao enviar a imagem.", "error");
    } finally {
      triggerButton.disabled = false;
      fileInput.value = "";
    }
  });

  setPreview(urlInput.value);
}

document.querySelectorAll("[data-image-upload]").forEach(initializeImageUploadField);
