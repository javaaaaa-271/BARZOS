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
const confirmationModal = document.getElementById("confirmation-modal");
const confirmationCode = document.getElementById("confirmation-code");
const confirmationText = document.getElementById("confirmation-text");
const checkoutPanel = document.querySelector(".checkout-panel");
const toggleCheckoutButton = document.getElementById("toggle-checkout");
const paymentMethodSelect = document.getElementById("payment-method");
const paymentMethodBadge = document.getElementById("payment-method-badge");
let checkoutCollapsed = false;

const paymentMethodLabels = {
  counter: "Pagar no balcao",
  pix: "Pix",
};

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

function buildSummary() {
  const selected = menuItems
    .map((item) => {
      const quantity = cart.get(String(item.id)) || 0;
      return quantity > 0 ? { ...item, quantity } : null;
    })
    .filter(Boolean);

  const totalItems = selected.reduce((sum, item) => sum + item.quantity, 0);
  orderCountBadge.textContent = `${totalItems} ${totalItems === 1 ? "item" : "itens"}`;
  checkoutPanel?.classList.toggle("has-items", totalItems > 0);
  if (paymentMethodBadge) {
    paymentMethodBadge.textContent = paymentMethodLabels[paymentMethodSelect?.value || "counter"] || "Pagar no balcao";
  }

  if (!selected.length) {
    orderSummary.className = "summary-list empty-state";
    orderSummary.textContent = "Selecione itens do cardapio para montar o pedido.";
    orderTotal.textContent = "R$ 0,00";
    submitOrderButton.disabled = true;
    updateCheckoutToggle(false);
    return;
  }

  const rows = selected.map((item) => {
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
  });

  const total = selected.reduce((sum, item) => sum + item.price * item.quantity, 0);
  orderSummary.className = "summary-list";
  orderSummary.innerHTML = rows.join("");
  orderTotal.textContent = currencyFormatter.format(total);
  submitOrderButton.disabled = false;
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

async function submitOrder() {
  const selectedItems = [...cart.entries()].map(([id, quantity]) => ({ id, quantity }));
  if (!selectedItems.length) {
    return;
  }

  const customerName = document.getElementById("customer-name").value.trim();
  const tableLabel = document.getElementById("table-label").value.trim();
  const paymentMethod = paymentMethodSelect?.value || "counter";

  submitOrderButton.disabled = true;
  submitOrderButton.textContent = "Enviando...";

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

  const data = await response.json();
  submitOrderButton.textContent = "Confirmar pedido";

  if (!response.ok) {
    submitOrderButton.disabled = false;
    alert(data.error || "Nao foi possivel enviar o pedido.");
    return;
  }

  confirmationCode.textContent = data.order.order_number || data.order.code;
  confirmationText.textContent = `Pedido de ${data.order.customer_name} enviado para ${data.order.table_label} com ${data.order.payment_method_label}.`;
  confirmationModal.classList.remove("hidden");
  confirmationModal.setAttribute("aria-hidden", "false");

  cart.clear();
  document.querySelectorAll(".menu-card").forEach((card) => updateCardQuantity(card, 0));
  buildSummary();
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

paymentMethodSelect?.addEventListener("change", buildSummary);

submitOrderButton?.addEventListener("click", submitOrder);

document.getElementById("close-confirmation")?.addEventListener("click", () => {
  confirmationModal.classList.add("hidden");
  confirmationModal.setAttribute("aria-hidden", "true");
});

buildSummary();
