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
const simulatePixPaymentButton = document.getElementById("simulate-pix-payment");
const closePixModalTopButton = document.getElementById("close-pix-modal-top");

let checkoutCollapsed = false;
let currentPixOrder = null;

const paymentMethodLabels = {
  counter: "Pagar no balcao",
  pix: "Pix",
};

const paymentMethodInstructions = {
  counter: "Pagamento sera feito no balcao ou no momento da retirada.",
  pix: "O pedido sera criado agora e fica aguardando a confirmacao do pagamento Pix.",
};

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
  toggleCheckoutButton.textContent = collapsed ? "Expandir" : "Minimizar";
  toggleCheckoutButton.setAttribute("aria-expanded", collapsed ? "false" : "true");
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
    checkoutInstruction.textContent = "Escolha uma forma de pagamento para liberar o envio do pedido.";
    return;
  }
  checkoutInstruction.textContent = paymentMethodInstructions[paymentMethod] || "Confirme o pagamento para seguir.";
}

function syncCheckoutReview() {
  const selected = getSelectedItems();
  const total = selected.reduce((sum, item) => sum + item.price * item.quantity, 0);

  if (!checkoutReviewSummary || !checkoutReviewTotal) {
    return;
  }

  if (!selected.length) {
    checkoutReviewSummary.className = "summary-list empty-state";
    checkoutReviewSummary.textContent = "Nenhum item selecionado.";
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
  checkoutPanel?.classList.toggle("has-items", totalItems > 0);

  if (!selected.length) {
    orderSummary.className = "summary-list empty-state";
    orderSummary.textContent = "Selecione itens do cardapio para montar o pedido.";
    orderTotal.textContent = "R$ 0,00";
    submitOrderButton.disabled = true;
    updateCheckoutToggle(false);
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
  setCheckoutError("");
  checkoutPaymentInputs.forEach((input) => {
    input.checked = false;
  });
  syncCheckoutReview();
  updateCheckoutInstruction();
  checkoutModal?.classList.remove("hidden");
  checkoutModal?.setAttribute("aria-hidden", "false");
}

function closeCheckoutModal() {
  checkoutModal?.classList.add("hidden");
  checkoutModal?.setAttribute("aria-hidden", "true");
  setCheckoutError("");
}

function closeConfirmationModal() {
  confirmationModal?.classList.add("hidden");
  confirmationModal?.setAttribute("aria-hidden", "true");
}

function closePixModal() {
  pixModal?.classList.add("hidden");
  pixModal?.setAttribute("aria-hidden", "true");
  setPixError("");
}

function resetCart() {
  cart.clear();
  document.querySelectorAll(".menu-card").forEach((card) => updateCardQuantity(card, 0));
  buildSummary();
}

function showConfirmation(order, nextStepText) {
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
  currentPixOrder = order;
  pixOrderCode.textContent = order.order_number || order.code;
  pixPaymentStatus.textContent = order.payment_status_label;
  pixPaymentInstruction.textContent = "Escaneie o QR code ou use o Pix copia e cola abaixo. Se precisar testar o fluxo, use o botao de simulacao.";
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
  if (!paymentMethod) {
    setCheckoutError("Escolha como voce vai pagar antes de enviar o pedido.");
    return;
  }

  const customerName = document.getElementById("customer-name").value.trim();
  const tableLabel = document.getElementById("table-label").value.trim();

  checkoutSubmitButton.disabled = true;
  checkoutSubmitButton.textContent = "Enviando...";
  setCheckoutError("");

  const response = await fetch("/api/orders", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      customer_name: customerName,
      table_label: tableLabel,
      source: "menu-digital",
      payment_method: paymentMethod,
      items: selectedItems,
    }),
  });

  const data = await response.json().catch(() => ({}));
  checkoutSubmitButton.disabled = false;
  checkoutSubmitButton.textContent = "Enviar pedido";

  if (!response.ok) {
    setCheckoutError(data.error || "Nao foi possivel enviar o pedido.");
    return;
  }

  closeCheckoutModal();
  resetCart();

  if (paymentMethod === "pix") {
    populatePixModal(data.order);
    return;
  }

  showConfirmation(data.order, "Seu pedido foi enviado. Pagamento sera feito no balcao.");
}

async function simulatePixPayment() {
  if (!currentPixOrder?.code) {
    setPixError("Pedido Pix nao encontrado.");
    return;
  }

  simulatePixPaymentButton.disabled = true;
  simulatePixPaymentButton.textContent = "Confirmando...";
  setPixError("");

  const response = await fetch(`/api/orders/${currentPixOrder.code}/pix/simulate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });

  const data = await response.json().catch(() => ({}));
  simulatePixPaymentButton.disabled = false;
  simulatePixPaymentButton.textContent = "Simular pagamento";

  if (!response.ok) {
    setPixError(data.error || "Nao foi possivel confirmar o pagamento Pix.");
    return;
  }

  currentPixOrder = data.order;
  closePixModal();
  showConfirmation(
    data.order,
    "Seu pedido foi enviado. O pagamento via Pix foi confirmado e o bar ja pode seguir com a liberacao."
  );
}

async function copyPixCode() {
  if (!pixCopyPaste?.value) {
    setPixError("Nao ha Pix copia e cola disponivel para este pedido.");
    return;
  }
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(pixCopyPaste.value);
      pixPaymentInstruction.textContent = "Pix copia e cola copiado. Voce pode pagar e depois confirmar a simulacao.";
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
simulatePixPaymentButton?.addEventListener("click", simulatePixPayment);
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

buildSummary();
