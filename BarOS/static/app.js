const menuItems = window.BAROS_MENU || [];
const cart = new Map();

const currencyFormatter = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
});

const orderSummary = document.getElementById("order-summary");
const orderTotal = document.getElementById("order-total");
const orderCountBadge = document.getElementById("order-count-badge");
const submitOrderButton = document.getElementById("submit-order");
const checkoutPanel = document.querySelector(".checkout-panel");
const toggleCheckoutButton = document.getElementById("toggle-checkout");
const openCheckoutQuickButton = document.getElementById("open-checkout-quick");
const customerNameInput = document.getElementById("customer-name");
const tableLabelInput = document.getElementById("table-label");
const checkoutFooterCount = document.getElementById("checkout-footer-count");

const checkoutModal = document.getElementById("checkout-modal");
const checkoutReviewSummary = document.getElementById("checkout-review-summary");
const checkoutReviewTotal = document.getElementById("checkout-review-total");
const checkoutInstruction = document.getElementById("checkout-instruction");
const checkoutError = document.getElementById("checkout-error");
const checkoutSubmitButton = document.getElementById("confirm-checkout-submit");
const closeCheckoutModalButton = document.getElementById("close-checkout-modal");
const closeCheckoutModalTopButton = document.getElementById("close-checkout-modal-top");
const checkoutPaymentInputs = Array.from(document.querySelectorAll('input[name="checkout-payment-method"]'));

const confirmationModal = document.getElementById("confirmation-modal");
const confirmationCode = document.getElementById("confirmation-code");
const confirmationText = document.getElementById("confirmation-text");

const pixModal = document.getElementById("pix-modal");
const pixOrderCode = document.getElementById("pix-order-code");
const pixPaymentStatus = document.getElementById("pix-payment-status");
const pixPaymentInstruction = document.getElementById("pix-payment-instruction");
const pixQrImage = document.getElementById("pix-qr-image");
const pixQrPlaceholder = document.getElementById("pix-qr-placeholder");
const pixCopyPaste = document.getElementById("pix-copy-paste");
const pixError = document.getElementById("pix-error");
const copyPixCodeButton = document.getElementById("copy-pix-code");
const closePixModalTopButton = document.getElementById("close-pix-modal-top");

let checkoutCollapsed = false;
let currentPixOrder = null;
let checkoutPanelWasCollapsedBeforeModal = false;
let checkoutPanelHiddenForFlow = false;
let lastResponsiveMobileState = null;

const paymentMethodLabels = {
  counter: "Pagar no balcao",
  pix: "Pix",
};

const paymentMethodInstructions = {
  counter: "Voce paga no balcao ou no momento da retirada.",
  pix: "Pague no Pix e aguarde a confirmacao do bar.",
};
const PENDING_ORDER_STORAGE_KEY = "baros_pending_order_v1";
const PENDING_ORDER_MAX_AGE_MS = 30 * 60 * 1000;

function logCheckoutFlow(step, details = {}) {
  console.info("[BarOS checkout]", step, details);
}

function isMobileViewport() {
  return window.matchMedia("(max-width: 640px)").matches;
}

function createRequestId() {
  if (window.crypto?.randomUUID) {
    return window.crypto.randomUUID();
  }
  return `baros-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
}

function normalizeItemsForIdempotency(items) {
  return [...(items || [])]
    .map((item) => ({
      id: Number(item.id),
      quantity: Number(item.quantity),
    }))
    .filter((item) => item.id > 0 && item.quantity > 0)
    .sort((left, right) => left.id - right.id);
}

function buildOrderFingerprint(payload) {
  return JSON.stringify({
    customer_name: (payload.customer_name || "").trim(),
    table_label: (payload.table_label || "").trim(),
    source: payload.source || "",
    payment_method: payload.payment_method || "",
    items: normalizeItemsForIdempotency(payload.items),
  });
}

function readPendingOrderDraft() {
  try {
    const raw = window.sessionStorage?.getItem(PENDING_ORDER_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const draft = JSON.parse(raw);
    if (!draft?.request_id || !draft?.payload || !draft?.created_at) {
      window.sessionStorage.removeItem(PENDING_ORDER_STORAGE_KEY);
      return null;
    }
    if (Date.now() - Number(draft.created_at) > PENDING_ORDER_MAX_AGE_MS) {
      window.sessionStorage.removeItem(PENDING_ORDER_STORAGE_KEY);
      return null;
    }
    return draft;
  } catch (error) {
    console.error(error);
    return null;
  }
}

function writePendingOrderDraft(draft) {
  try {
    window.sessionStorage?.setItem(PENDING_ORDER_STORAGE_KEY, JSON.stringify(draft));
  } catch (error) {
    console.error(error);
  }
}

function clearPendingOrderDraft() {
  try {
    window.sessionStorage?.removeItem(PENDING_ORDER_STORAGE_KEY);
  } catch (error) {
    console.error(error);
  }
}

function getOrCreateRequestId(payload) {
  const fingerprint = buildOrderFingerprint(payload);
  const existingDraft = readPendingOrderDraft();
  if (existingDraft?.fingerprint === fingerprint && existingDraft?.request_id) {
    return {
      requestId: existingDraft.request_id,
      fingerprint,
    };
  }
  return {
    requestId: createRequestId(),
    fingerprint,
  };
}

function restorePendingOrderDraft() {
  const draft = readPendingOrderDraft();
  if (!draft?.payload) {
    return;
  }

  cart.clear();
  normalizeItemsForIdempotency(draft.payload.items).forEach((item) => {
    cart.set(String(item.id), item.quantity);
  });

  document.querySelectorAll(".menu-card").forEach((card) => {
    updateCardQuantity(card, cart.get(card.dataset.menuId) || 0);
  });

  if (customerNameInput) {
    customerNameInput.value = draft.payload.customer_name || "";
  }
  if (tableLabelInput) {
    tableLabelInput.value = draft.payload.table_label || "";
  }
  checkoutPaymentInputs.forEach((input) => {
    input.checked = input.value === draft.payload.payment_method;
  });
}

function getSelectedItems() {
  return menuItems
    .map((item) => {
      const quantity = cart.get(String(item.id)) || 0;
      return quantity > 0 ? { ...item, quantity } : null;
    })
    .filter(Boolean);
}

function getSelectedPaymentMethod() {
  const selected = checkoutPaymentInputs.find((input) => input.checked);
  return selected ? selected.value : "";
}

function setCheckoutError(message = "") {
  if (!checkoutError) {
    return;
  }
  checkoutError.textContent = message;
  checkoutError.classList.toggle("hidden", !message);
}

function setPixError(message = "") {
  if (!pixError) {
    return;
  }
  pixError.textContent = message;
  pixError.classList.toggle("hidden", !message);
}

function updateCardQuantity(card, nextQuantity) {
  card.querySelector(".qty-value").textContent = String(nextQuantity);
}

function updateCheckoutToggle(collapsed) {
  if (!toggleCheckoutButton || !checkoutPanel) {
    return;
  }
  checkoutCollapsed = collapsed;
  checkoutPanel.classList.toggle("is-collapsed", collapsed);
  toggleCheckoutButton.textContent = isMobileViewport()
    ? (collapsed ? "Ver itens" : "Ocultar itens")
    : (collapsed ? "Expandir" : "Minimizar");
  toggleCheckoutButton.setAttribute("aria-expanded", collapsed ? "false" : "true");
}

function applyResponsiveCheckoutMode() {
  const isMobile = isMobileViewport();
  if (lastResponsiveMobileState === isMobile) {
    return;
  }
  lastResponsiveMobileState = isMobile;
  if (checkoutPanelHiddenForFlow) {
    return;
  }
  updateCheckoutToggle(isMobile);
}

function hideCheckoutPanelForFlow() {
  if (!checkoutPanel || checkoutPanelHiddenForFlow) {
    return;
  }
  checkoutPanelWasCollapsedBeforeModal = checkoutCollapsed;
  checkoutPanelHiddenForFlow = true;
  checkoutPanel.classList.add("is-flow-hidden");
  logCheckoutFlow("hide_checkout_panel_for_flow", {
    wasCollapsed: checkoutPanelWasCollapsedBeforeModal,
  });
}

function showCheckoutPanelAfterFlow() {
  if (!checkoutPanel || !checkoutPanelHiddenForFlow) {
    return;
  }
  checkoutPanelHiddenForFlow = false;
  checkoutPanel.classList.remove("is-flow-hidden");
  updateCheckoutToggle(checkoutPanelWasCollapsedBeforeModal);
  logCheckoutFlow("show_checkout_panel_after_flow", {
    restoredCollapsed: checkoutPanelWasCollapsedBeforeModal,
  });
}

function renderSummaryRows(selected) {
  return selected
    .map((item) => {
      const subtotal = item.price * item.quantity;
      return `
        <div class="summary-item">
          <div>
            <strong>${item.name}</strong>
            <div class="muted-note">${item.quantity}x selecionado</div>
          </div>
          <strong>${currencyFormatter.format(subtotal)}</strong>
        </div>
      `;
    })
    .join("");
}

function updateCheckoutInstruction() {
  const paymentMethod = getSelectedPaymentMethod();
  if (!paymentMethod) {
    checkoutInstruction.textContent = "Escolha como pagar para liberar o envio.";
    return;
  }
  checkoutInstruction.textContent = paymentMethodInstructions[paymentMethod] || "Confirme como vai pagar para seguir.";
}

function syncCheckoutReview() {
  const selected = getSelectedItems();
  const total = selected.reduce((sum, item) => sum + item.price * item.quantity, 0);

  if (!checkoutReviewSummary || !checkoutReviewTotal) {
    return;
  }

  if (!selected.length) {
    checkoutReviewSummary.className = "summary-list empty-state";
    checkoutReviewSummary.textContent = "Seu carrinho esta vazio.";
    checkoutReviewTotal.textContent = "R$ 0,00";
    return;
  }

  checkoutReviewSummary.className = "summary-list";
  checkoutReviewSummary.innerHTML = renderSummaryRows(selected);
  checkoutReviewTotal.textContent = currencyFormatter.format(total);
}

function buildSummary() {
  const selected = getSelectedItems();
  const totalItems = selected.reduce((sum, item) => sum + item.quantity, 0);

  orderCountBadge.textContent = `${totalItems} ${totalItems === 1 ? "item" : "itens"}`;
  if (checkoutFooterCount) {
    checkoutFooterCount.textContent = totalItems
      ? `${totalItems} ${totalItems === 1 ? "item" : "itens"}`
      : "Carrinho vazio";
  }
  checkoutPanel?.classList.toggle("has-items", totalItems > 0);

  if (!selected.length) {
    orderSummary.className = "summary-list empty-state";
    orderSummary.textContent = "Toque em + para adicionar bebidas ao carrinho.";
    orderTotal.textContent = "R$ 0,00";
    submitOrderButton.disabled = true;
    if (!isMobileViewport()) {
      updateCheckoutToggle(false);
    }
    syncCheckoutReview();
    return;
  }

  const total = selected.reduce((sum, item) => sum + item.price * item.quantity, 0);
  orderSummary.className = "summary-list";
  orderSummary.innerHTML = renderSummaryRows(selected);
  orderTotal.textContent = currencyFormatter.format(total);
  submitOrderButton.disabled = false;
  syncCheckoutReview();
}

function handleQuantityChange(card, delta) {
  if (card.dataset.available !== "true") {
    return;
  }

  const itemId = card.dataset.menuId;
  const current = cart.get(itemId) || 0;
  const next = Math.max(0, current + delta);
  if (next === 0) {
    cart.delete(itemId);
  } else {
    cart.set(itemId, next);
  }
  updateCardQuantity(card, next);
  buildSummary();
}

function openCheckoutModal() {
  const selected = getSelectedItems();
  if (!selected.length) {
    return;
  }
  logCheckoutFlow("open_checkout_modal", {
    itemCount: selected.reduce((sum, item) => sum + item.quantity, 0),
    total: selected.reduce((sum, item) => sum + item.price * item.quantity, 0),
  });
  setCheckoutError("");
  checkoutPaymentInputs.forEach((input) => {
    input.checked = false;
  });
  syncCheckoutReview();
  updateCheckoutInstruction();
  hideCheckoutPanelForFlow();
  checkoutModal?.classList.remove("hidden");
  checkoutModal?.setAttribute("aria-hidden", "false");
}

function closeCheckoutModal({ restorePanel = true } = {}) {
  checkoutModal?.classList.add("hidden");
  checkoutModal?.setAttribute("aria-hidden", "true");
  setCheckoutError("");
  if (restorePanel) {
    showCheckoutPanelAfterFlow();
  }
}

function closeConfirmationModal() {
  confirmationModal?.classList.add("hidden");
  confirmationModal?.setAttribute("aria-hidden", "true");
  showCheckoutPanelAfterFlow();
}

function closePixModal({ restorePanel = true } = {}) {
  pixModal?.classList.add("hidden");
  pixModal?.setAttribute("aria-hidden", "true");
  setPixError("");
  if (restorePanel) {
    showCheckoutPanelAfterFlow();
  }
}

function resetCart() {
  cart.clear();
  document.querySelectorAll(".menu-card").forEach((card) => updateCardQuantity(card, 0));
  buildSummary();
}

function showConfirmation(order, nextStepText) {
  logCheckoutFlow("show_confirmation", {
    code: order.code,
    orderNumber: order.order_number,
    paymentMethod: order.payment_method,
    paymentStatus: order.payment_status,
  });
  confirmationCode.textContent = order.order_number || order.code;
  confirmationText.innerHTML = `
    <strong>${nextStepText}</strong><br>
    Forma de pagamento: ${order.payment_method_label}.<br>
    Status do pagamento: ${order.payment_status_label}.<br>
    Proximo passo: acompanhe o preparo e siga a instrucao indicada.
  `;
  confirmationModal.classList.remove("hidden");
  confirmationModal.setAttribute("aria-hidden", "false");
}

function populatePixModal(order) {
  logCheckoutFlow("open_pix_modal", {
    code: order.code,
    paymentMethod: order.payment_method,
    paymentStatus: order.payment_status,
    hasQr: Boolean(order.pix_qr_code),
    hasCopyPaste: Boolean(order.pix_copy_paste),
  });
  currentPixOrder = order;
  pixOrderCode.textContent = order.order_number || order.code;
  pixPaymentStatus.textContent = order.payment_status_label;
  pixPaymentInstruction.textContent = "Escaneie o QR code ou copie a chave Pix abaixo. Depois do pagamento, o bar confirma internamente.";
  pixCopyPaste.value = order.pix_copy_paste || "";

  if (order.pix_qr_code) {
    pixQrImage.src = order.pix_qr_code;
    pixQrImage.classList.remove("hidden");
    pixQrPlaceholder.classList.add("hidden");
  } else {
    pixQrImage.removeAttribute("src");
    pixQrImage.classList.add("hidden");
    pixQrPlaceholder.classList.remove("hidden");
    pixQrPlaceholder.textContent = "PIX";
  }

  pixModal?.classList.remove("hidden");
  pixModal?.setAttribute("aria-hidden", "false");
}

async function submitOrder() {
  const selectedItems = [...cart.entries()].map(([id, quantity]) => ({ id, quantity }));
  if (!selectedItems.length) {
    return;
  }

  const paymentMethod = getSelectedPaymentMethod();
  logCheckoutFlow("selected_payment_method", { paymentMethod });
  if (!paymentMethod) {
    setCheckoutError("Escolha como voce vai pagar antes de enviar o pedido.");
    return;
  }

  const customerName = customerNameInput?.value.trim() || "";
  const tableLabel = tableLabelInput?.value.trim() || "";

  checkoutSubmitButton.disabled = true;
  checkoutSubmitButton.textContent = "Enviando...";
  setCheckoutError("");

  const payload = {
    customer_name: customerName,
    table_label: tableLabel,
    source: "menu-digital",
    payment_method: paymentMethod,
    items: selectedItems,
  };
  const { requestId, fingerprint } = getOrCreateRequestId(payload);
  payload.request_id = requestId;
  writePendingOrderDraft({
    request_id: requestId,
    fingerprint,
    payload,
    created_at: Date.now(),
  });
  logCheckoutFlow("submit_order_payload", payload);

  let response;
  try {
    response = await fetch("/api/orders", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": requestId,
      },
      body: JSON.stringify(payload),
    });
  } catch (error) {
    console.error(error);
    checkoutSubmitButton.disabled = false;
    checkoutSubmitButton.textContent = "Enviar pedido";
    setCheckoutError("Nao foi possivel enviar agora. Tente novamente que o sistema reaproveita a mesma tentativa com seguranca.");
    return;
  }

  const data = await response.json().catch(() => ({}));
  logCheckoutFlow("submit_order_response", {
    ok: response.ok,
    status: response.status,
    data,
  });
  checkoutSubmitButton.disabled = false;
  checkoutSubmitButton.textContent = "Enviar pedido";

  if (!response.ok) {
    setCheckoutError(data.error || "Nao foi possivel enviar o pedido.");
    return;
  }

  clearPendingOrderDraft();
  closeCheckoutModal({ restorePanel: false });
  resetCart();

  if (paymentMethod === "pix") {
    logCheckoutFlow("submit_order_branch", { branch: "pix", code: data.order?.code });
    populatePixModal(data.order);
    return;
  }

  logCheckoutFlow("submit_order_branch", { branch: "counter", code: data.order?.code });
  showConfirmation(data.order, "Seu pedido foi enviado. Pagamento sera feito no balcao.");
}

async function copyPixCode() {
  if (!pixCopyPaste?.value) {
    setPixError("Nao ha Pix copia e cola disponivel para este pedido.");
    return;
  }
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(pixCopyPaste.value);
      pixPaymentInstruction.textContent = "Pix copia e cola copiado. Depois do pagamento, o bar confirma internamente.";
      return;
    }
  } catch (error) {
    console.error(error);
  }
  pixCopyPaste.focus();
  pixCopyPaste.select();
  pixPaymentInstruction.textContent = "Selecione e copie manualmente o Pix copia e cola.";
}

document.querySelectorAll(".menu-card").forEach((card) => {
  card.addEventListener("click", (event) => {
    const button = event.target.closest(".qty-btn");
    if (!button) {
      return;
    }
    const delta = button.dataset.action === "increase" ? 1 : -1;
    handleQuantityChange(card, delta);
  });
});

toggleCheckoutButton?.addEventListener("click", () => {
  updateCheckoutToggle(!checkoutCollapsed);
});

openCheckoutQuickButton?.addEventListener("click", () => {
  updateCheckoutToggle(false);
  checkoutPanel?.scrollIntoView({ behavior: "smooth", block: "nearest" });
});

submitOrderButton?.addEventListener("click", openCheckoutModal);
checkoutSubmitButton?.addEventListener("click", submitOrder);
closeCheckoutModalButton?.addEventListener("click", closeCheckoutModal);
closeCheckoutModalTopButton?.addEventListener("click", closeCheckoutModal);
checkoutModal?.addEventListener("click", (event) => {
  if (event.target === checkoutModal) {
    closeCheckoutModal();
  }
});

checkoutPaymentInputs.forEach((input) => {
  input.addEventListener("change", () => {
    setCheckoutError("");
    updateCheckoutInstruction();
  });
});

document.getElementById("close-confirmation")?.addEventListener("click", closeConfirmationModal);
copyPixCodeButton?.addEventListener("click", copyPixCode);
closePixModalTopButton?.addEventListener("click", closePixModal);
pixModal?.addEventListener("click", (event) => {
  if (event.target === pixModal) {
    closePixModal();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    if (!checkoutModal?.classList.contains("hidden")) {
      closeCheckoutModal();
      return;
    }
    if (!pixModal?.classList.contains("hidden")) {
      closePixModal();
      return;
    }
    if (!confirmationModal?.classList.contains("hidden")) {
      closeConfirmationModal();
    }
  }
});

restorePendingOrderDraft();
applyResponsiveCheckoutMode();
buildSummary();
updateCheckoutInstruction();
window.addEventListener("resize", applyResponsiveCheckoutMode);
