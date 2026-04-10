const PUBLIC_ORDER_STORAGE_KEY = "baros_public_order_v1";
const publicOrderState = {
  order: window.BAROS_PUBLIC_ORDER || null,
  timer: null,
};

const publicOrderNumber = document.getElementById("public-order-number");
const publicOrderCode = document.getElementById("public-order-code");
const publicOrderStateLabel = document.getElementById("public-order-state");
const publicOrderMessage = document.getElementById("public-order-message");
const publicPaymentStatus = document.getElementById("public-payment-status");
const publicPickupCode = document.getElementById("public-pickup-code");
const pickupCodeShell = document.getElementById("pickup-code-shell");
const publicCountdown = document.getElementById("public-countdown");
const publicCountdownShell = document.getElementById("public-countdown-shell");
const publicPixStatus = document.getElementById("public-pix-status");
const publicPixInstruction = document.getElementById("public-pix-instruction");
const publicPixQr = document.getElementById("public-pix-qr");
const publicPixPlaceholder = document.getElementById("public-pix-placeholder");
const publicPixCopyPaste = document.getElementById("public-pix-copy-paste");
const publicOrderItems = document.getElementById("public-order-items");
const publicOrderFeedback = document.getElementById("public-order-feedback");
const publicGeneratedAt = document.getElementById("public-order-generated-at");
const publicCopyPixButton = document.getElementById("public-copy-pix");
const publicRefreshButton = document.getElementById("public-refresh-order");
const publicRegenerateButton = document.getElementById("public-regenerate-pix");

function setPublicOrderFeedback(message = "", isError = false) {
  if (!publicOrderFeedback) {
    return;
  }
  publicOrderFeedback.textContent = message;
  publicOrderFeedback.classList.toggle("hidden", !message);
  publicOrderFeedback.classList.toggle("critical-alert", Boolean(message && isError));
}

function savePublicOrder(order) {
  if (!order?.public_token) {
    return;
  }
  try {
    window.localStorage?.setItem(
      PUBLIC_ORDER_STORAGE_KEY,
      JSON.stringify({
        public_token: order.public_token,
        code: order.code,
        order_number: order.order_number,
        public_url: order.public_url,
        updated_at: Date.now(),
      }),
    );
  } catch (error) {
    console.error(error);
  }
}

function formatCurrency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(Number(value || 0));
}

function formatCountdown(expiresAt) {
  if (!expiresAt) {
    return "--:--";
  }
  const remainingMs = new Date(expiresAt).getTime() - Date.now();
  if (remainingMs <= 0) {
    return "00:00";
  }
  const totalSeconds = Math.floor(remainingMs / 1000);
  const minutes = String(Math.floor(totalSeconds / 60)).padStart(2, "0");
  const seconds = String(totalSeconds % 60).padStart(2, "0");
  return `${minutes}:${seconds}`;
}

function renderPublicOrderItems(items) {
  if (!publicOrderItems) {
    return;
  }
  publicOrderItems.innerHTML = (items || [])
    .map(
      (item) => `
        <div class="summary-line">
          <span>${item.quantity}x ${item.name}</span>
          <strong>${formatCurrency(item.subtotal)}</strong>
        </div>
      `,
    )
    .join("");
}

function updateCountdown() {
  const order = publicOrderState.order;
  if (!order || !publicCountdown) {
    return;
  }
  publicCountdown.textContent = formatCountdown(order.pix_expires_at);
}

function schedulePublicOrderRefresh() {
  window.clearTimeout(publicOrderState.timer);
  const order = publicOrderState.order;
  if (!order) {
    return;
  }
  const delay =
    order.public_state === "awaiting_payment" ? 5000 : order.public_state === "paid" ? 15000 : 30000;
  publicOrderState.timer = window.setTimeout(() => {
    void refreshPublicOrderStatus();
  }, delay);
}

function renderPublicOrder(order) {
  if (!order) {
    return;
  }
  publicOrderState.order = order;
  savePublicOrder(order);
  publicOrderNumber.textContent = order.order_number || order.code;
  publicOrderCode.textContent = order.order_number || order.code;
  publicOrderStateLabel.textContent = order.public_state_label;
  publicOrderMessage.textContent = order.public_message;
  publicPaymentStatus.textContent = order.payment_status_label;
  publicPixStatus.textContent = order.public_state_label;
  publicPixInstruction.textContent = order.public_message;
  publicPixCopyPaste.value = order.pix_copy_paste || "";
  publicGeneratedAt.textContent = new Date().toLocaleString("pt-BR");
  pickupCodeShell?.classList.toggle("hidden", !order.pickup_code);
  if (publicPickupCode) {
    publicPickupCode.textContent = order.pickup_code || "------";
  }
  publicCountdownShell?.classList.toggle(
    "hidden",
    !(order.public_state === "awaiting_payment" && order.pix_expires_at),
  );
  updateCountdown();
  if (order.pix_qr_code) {
    publicPixQr?.classList.remove("hidden");
    publicPixQr.src = order.pix_qr_code;
    publicPixPlaceholder?.classList.add("hidden");
  } else {
    publicPixQr?.classList.add("hidden");
    publicPixQr?.removeAttribute("src");
    publicPixPlaceholder?.classList.remove("hidden");
  }
  publicRegenerateButton?.classList.toggle("hidden", !order.can_regenerate_pix);
  renderPublicOrderItems(order.items);
  schedulePublicOrderRefresh();
}

async function refreshPublicOrderStatus({ silent = false } = {}) {
  const order = publicOrderState.order;
  if (!order?.status_url) {
    return;
  }
  if (!silent) {
    setPublicOrderFeedback("Atualizando status...");
  }
  try {
    const response = await fetch(order.status_url, { method: "GET" });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.error || "Nao foi possivel atualizar o status.");
    }
    renderPublicOrder(data.order);
    setPublicOrderFeedback("Status atualizado.");
  } catch (error) {
    console.error(error);
    setPublicOrderFeedback(error.message || "Nao foi possivel atualizar o status.", true);
  }
}

async function copyPublicPixCode() {
  const value = publicPixCopyPaste?.value || "";
  if (!value) {
    setPublicOrderFeedback("Nao ha Pix copia e cola disponivel.", true);
    return;
  }
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      setPublicOrderFeedback("Pix copia e cola copiado.");
      return;
    }
  } catch (error) {
    console.error(error);
  }
  publicPixCopyPaste.focus();
  publicPixCopyPaste.select();
  setPublicOrderFeedback("Selecione e copie manualmente o Pix.");
}

async function regeneratePix() {
  const order = publicOrderState.order;
  if (!order?.code || !order?.public_token) {
    return;
  }
  setPublicOrderFeedback("Gerando novo Pix...");
  publicRegenerateButton.disabled = true;
  try {
    const response = await fetch(`/api/orders/${encodeURIComponent(order.code)}/pix/regenerate`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ public_token: order.public_token }),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.error || "Nao foi possivel gerar um novo Pix.");
    }
    renderPublicOrder(data.order);
    setPublicOrderFeedback("Novo Pix gerado com sucesso.");
  } catch (error) {
    console.error(error);
    setPublicOrderFeedback(error.message || "Nao foi possivel gerar um novo Pix.", true);
  } finally {
    publicRegenerateButton.disabled = false;
  }
}

publicCopyPixButton?.addEventListener("click", () => {
  void copyPublicPixCode();
});

publicRefreshButton?.addEventListener("click", () => {
  void refreshPublicOrderStatus();
});

publicRegenerateButton?.addEventListener("click", () => {
  void regeneratePix();
});

window.setInterval(updateCountdown, 1000);
renderPublicOrder(publicOrderState.order);
