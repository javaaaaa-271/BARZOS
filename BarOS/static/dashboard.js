const dashboardConfig = window.BAROS_DASHBOARD || {};
const canManageBar = Boolean(dashboardConfig.canManageBar);

const pendingContainer = document.getElementById("pending-orders");
const awaitingPaymentContainer = document.getElementById("awaiting-payment-orders");
const completedContainer = document.getElementById("completed-orders");
const topItemsContainer = document.getElementById("top-items");
const topTablesContainer = document.getElementById("top-tables");
const inventoryContainer = document.getElementById("inventory-items");
const notesContainer = document.getElementById("shift-notes");
const shiftHistoryContainer = document.getElementById("shift-history");
const refreshStatus = document.getElementById("refresh-status");
const closeBarButton = document.getElementById("close-bar-button");
const resetDataButton = document.getElementById("reset-data-button");
const closeoutModal = document.getElementById("closeout-modal");
const closeoutContent = document.getElementById("closeout-content");
const closeoutDrinks = document.getElementById("closeout-drinks");
const closeoutExportLink = document.getElementById("closeout-export-link");
const closeCloseoutModalButton = document.getElementById("close-closeout-modal");
const closeCloseoutModalTopButton = document.getElementById("close-closeout-modal-top");

let latestClosedShiftId = null;
let currentShiftId = dashboardConfig.currentShiftId || null;
let closeoutRequestInFlight = false;
let resetRequestInFlight = false;

const brl = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
});

function closeCloseoutModal() {
  closeoutModal?.classList.add("hidden");
  closeoutModal?.setAttribute("aria-hidden", "true");
}

function renderOrderItems(items) {
  return items
    .map((item) => `<li>${item.quantity}x ${item.name} <strong>${brl.format(item.subtotal)}</strong></li>`)
    .join("");
}

function renderOrderBadges(order) {
  return `
    <div class="order-badges">
      <span class="status-pill neutral">${order.status_label}</span>
      <span class="status-pill neutral">${order.order_type_label}</span>
      <span class="status-pill neutral">${order.payment_method_label}</span>
      <span class="status-pill ${order.payment_status === "paid" ? "ok" : order.payment_status === "failed" ? "critical" : "attention"}">${order.payment_status_label}</span>
    </div>
  `;
}

function renderPending(orders) {
  if (!orders.length) {
    pendingContainer.innerHTML = '<p class="empty-inline">Nenhum pedido aguardando preparo.</p>';
    return;
  }

  pendingContainer.innerHTML = orders
    .map(
      (order) => `
      <article class="order-card">
        <div class="order-meta">
          <strong>Pedido ${order.order_number || order.code}</strong>
          <span class="pill">${order.created_at}</span>
        </div>
        <h3>${order.customer_name}</h3>
        <p>${order.table_label}</p>
        ${renderOrderBadges(order)}
        <ul>${renderOrderItems(order.items)}</ul>
        <div class="order-actions">
          <span class="muted-note">Total ${brl.format(order.total)}</span>
          <div class="order-actions-group">
            ${
              order.payment_status !== "paid"
                ? `<button class="ghost-button small-button" data-pay="${order.code}" type="button">Marcar pago</button>`
                : ""
            }
            <a class="ghost-button small-button" href="/pedidos/${order.code}/imprimir" target="_blank" rel="noopener">Imprimir</a>
            <button class="primary-button" data-complete="${order.code}" type="button">Marcar como entregue</button>
          </div>
        </div>
      </article>
    `
    )
    .join("");
}

function renderCompleted(orders) {
  if (!orders.length) {
    completedContainer.innerHTML = '<p class="empty-inline">Os pedidos finalizados aparecerao aqui.</p>';
    return;
  }

  completedContainer.innerHTML = orders
    .map(
      (order) => `
      <article class="order-card">
        <div class="order-meta">
          <strong>Pedido ${order.order_number || order.code}</strong>
          <span class="status-badge">Concluido</span>
        </div>
        <h3>${order.customer_name}</h3>
        <p>${order.table_label} / concluido em ${order.completed_at || order.created_at}</p>
        ${renderOrderBadges(order)}
        <ul>${renderOrderItems(order.items)}</ul>
        <div class="order-actions">
          <span class="muted-note">Total ${brl.format(order.total)}</span>
          <div class="order-actions-group">
            ${
              order.payment_status !== "paid"
                ? `<button class="ghost-button small-button" data-pay="${order.code}" type="button">Marcar pago</button>`
                : ""
            }
            <a class="ghost-button small-button" href="/pedidos/${order.code}/imprimir" target="_blank" rel="noopener">Imprimir</a>
          </div>
        </div>
      </article>
    `
    )
    .join("");
}

function renderAwaitingPayment(orders) {
  if (!awaitingPaymentContainer) {
    return;
  }
  if (!orders.length) {
    awaitingPaymentContainer.innerHTML = '<p class="empty-inline">Nenhum pedido aguardando Pix no momento.</p>';
    return;
  }

  awaitingPaymentContainer.innerHTML = orders
    .map(
      (order) => `
      <article class="order-card">
        <div class="order-meta">
          <strong>Pedido ${order.order_number || order.code}</strong>
          <span class="pill">${order.created_at}</span>
        </div>
        <h3>${order.customer_name}</h3>
        <p>${order.table_label}</p>
        ${renderOrderBadges(order)}
        <ul>${renderOrderItems(order.items)}</ul>
        <div class="order-actions">
          <span class="muted-note">Aguardando confirmacao do Pix para liberar ao bar.</span>
          <div class="order-actions-group">
            <button class="ghost-button small-button" data-pay="${order.code}" type="button">Confirmar pagamento</button>
          </div>
        </div>
      </article>
    `
    )
    .join("");
}

function renderTopItems(items) {
  if (!items.length) {
    topItemsContainer.innerHTML = '<p class="empty-inline">Os destaques aparecem quando houver pedidos.</p>';
    return;
  }

  topItemsContainer.innerHTML = items
    .map((item) => `<div class="insight-row"><span>${item.name}</span><strong>${item.quantity}x</strong></div>`)
    .join("");
}

function renderTopTables(tables) {
  if (!tables.length) {
    topTablesContainer.innerHTML = '<p class="empty-inline">As mesas aparecem quando houver pedidos.</p>';
    return;
  }

  topTablesContainer.innerHTML = tables
    .map((table) => `<div class="insight-row"><span>${table.table_label}</span><strong>${table.total}</strong></div>`)
    .join("");
}

function renderInventoryControls(item) {
  if (!canManageBar) {
    return "";
  }

  return `
    <label class="inventory-input-group">
      <span>Qtd.</span>
      <input class="inventory-amount-input" type="number" min="0" step="0.1" value="1">
    </label>
    <button class="ghost-button small-button" type="button" data-stock-action="add">Repor</button>
    <button class="ghost-button small-button" type="button" data-stock-action="set">Ajustar</button>
    <label class="inventory-input-group">
      <span>Min.</span>
      <input class="inventory-par-input" type="number" min="0.1" step="0.1" value="${item.par_level}">
    </label>
    <button class="ghost-button small-button" type="button" data-save-par="true">Salvar minimo</button>
    <button class="ghost-button small-button" type="button" data-status="ok">OK</button>
    <button class="ghost-button small-button" type="button" data-status="attention">Atencao</button>
    <button class="ghost-button small-button" type="button" data-status="critical">Critico</button>
  `;
}

function renderInventory(items) {
  if (!items.length) {
    inventoryContainer.innerHTML = '<p class="empty-inline">Sem itens logisticos cadastrados.</p>';
    return;
  }

  inventoryContainer.innerHTML = items
    .map(
      (item) => `
      <article class="inventory-row" data-item-id="${item.id}">
        <div class="inventory-copy">
          <strong>${item.name}</strong>
          <p>${item.category} / ${item.stock_level} ${item.unit} / minimo ${item.par_level}</p>
          <span class="muted-note">Atualizado ${item.updated_at}</span>
        </div>
        <div class="inventory-actions">
          <span class="status-pill ${item.status}">${item.status}</span>
          ${renderInventoryControls(item)}
        </div>
      </article>
    `
    )
    .join("");
}

function renderNotes(notes) {
  if (!notes.length) {
    notesContainer.innerHTML = '<p class="empty-inline">Sem notas operacionais no turno.</p>';
    return;
  }

  notesContainer.innerHTML = notes
    .map(
      (note) => `
      <article class="note-row" data-note-id="${note.id}">
        <div>
          <strong>${note.title}</strong>
          <p>${note.body}</p>
        </div>
        <div class="note-actions">
          <span class="priority-pill ${note.priority}">${note.priority}</span>
          ${
            note.status !== "done"
              ? `<button class="primary-button small-button" type="button" data-close-note="${note.id}">Concluir</button>`
              : '<span class="status-badge">Concluido</span>'
          }
        </div>
      </article>
    `
    )
    .join("");
}

function renderShiftHistory(shifts) {
  if (!shiftHistoryContainer) {
    return;
  }

  if (!shifts.length) {
    shiftHistoryContainer.innerHTML = '<p class="empty-inline">Os turnos fechados vao aparecer aqui com resumo e exportacao.</p>';
    return;
  }

  shiftHistoryContainer.innerHTML = shifts
    .map((shift) => {
      const totalVendido = Number(shift.summary.total_vendido || 0);
      const totalPedidos = Number(shift.summary.total_pedidos || 0);
      const ticketMedio = Number(shift.summary.ticket_medio || 0);
      const lucroEstimado = Number(shift.summary.lucro_estimado || 0);
      const bebidaMaisVendida = shift.summary.bebida_mais_vendida?.name || "Sem dados";
      const horarioPico = shift.summary.horario_pico?.label || "Sem dados";
      const observacoes = Array.isArray(shift.observations) && shift.observations.length
        ? shift.observations.join(" / ")
        : "Sem observacoes registradas.";

      return `
        <article class="shift-card" data-shift-id="${shift.id}">
          <div class="shift-card-top">
            <div>
              <strong>Turno ${shift.id}</strong>
              <p>${shift.opened_at} ate ${shift.closed_at}</p>
            </div>
            <span class="pill">${shift.duration}</span>
          </div>
          <div class="shift-stats">
            <div class="summary-item">
              <div><strong>Total recebido</strong></div>
              <strong>${brl.format(totalVendido)}</strong>
            </div>
            <div class="summary-item">
              <div><strong>Pedidos</strong></div>
              <strong>${totalPedidos}</strong>
            </div>
            <div class="summary-item">
              <div><strong>Ticket medio</strong></div>
              <strong>${brl.format(ticketMedio)}</strong>
            </div>
            <div class="summary-item">
              <div><strong>Bebida lider</strong></div>
              <strong>${bebidaMaisVendida}</strong>
            </div>
            <div class="summary-item">
              <div><strong>Horario de pico</strong></div>
              <strong>${horarioPico}</strong>
            </div>
            <div class="summary-item">
              <div><strong>Lucro estimado</strong></div>
              <strong>${brl.format(lucroEstimado)}</strong>
            </div>
          </div>
          <div class="shift-observations-inline">
            <strong>Observacoes</strong>
            <p>${observacoes}</p>
          </div>
          ${
            `<div class="shift-actions">
              <a class="primary-button small-button" href="/historico-turnos/${shift.id}">Ver detalhes</a>
              ${
                canManageBar
                  ? `<a class="secondary-button small-button" href="/api/reports/shifts/${shift.id}/export">Exportar CSV</a>`
                  : ""
              }
            </div>`
          }
        </article>
      `;
    })
    .join("");
}

function updateSummary(summary) {
  document.getElementById("pending-count").textContent = summary.pending_count;
  document.getElementById("completed-count").textContent = summary.completed_count;
  document.getElementById("awaiting-payment-count").textContent = summary.awaiting_payment_count || 0;
  document.getElementById("total-count").textContent = summary.total_count;
  document.getElementById("revenue-total").textContent = brl.format(summary.revenue);
  document.getElementById("average-ticket").textContent = brl.format(summary.average_ticket);
  const peakTime = document.getElementById("peak-time");
  if (peakTime) {
    peakTime.textContent = summary.peak_time_label || "Sem dados";
  }
}

function updateLogistics(logistics) {
  document.getElementById("critical-count").textContent = logistics.inventory_summary.critical_count;
  document.getElementById("attention-count").textContent = logistics.inventory_summary.attention_count;
  document.getElementById("tracked-count").textContent = logistics.inventory_summary.tracked_count;
  renderInventory(logistics.inventory);
  renderNotes(logistics.notes);
}

function setCloseoutExportLink(shiftId) {
  if (!closeoutExportLink) {
    return;
  }

  latestClosedShiftId = shiftId;
  if (shiftId) {
    closeoutExportLink.href = `/api/reports/shifts/${shiftId}/export`;
    closeoutExportLink.classList.remove("hidden-link");
  } else {
    closeoutExportLink.href = "#";
    closeoutExportLink.classList.add("hidden-link");
  }
}

function setButtonBusy(button, busy, busyLabel, idleLabel) {
  if (!button) {
    return;
  }
  button.disabled = busy;
  button.textContent = busy ? busyLabel : idleLabel;
}

async function refreshDashboard() {
  const response = await fetch("/api/orders");
  if (!response.ok) {
    refreshStatus.textContent = "Falha ao atualizar";
    return;
  }

  const data = await response.json();
  applyDashboardSnapshot(data);
}

async function completeOrder(code) {
  const response = await fetch(`/api/orders/${code}/complete`, { method: "POST" });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    refreshStatus.textContent = data.error || "Nao foi possivel concluir";
    return;
  }
  await refreshDashboard();
}

async function payOrder(code) {
  const response = await fetch(`/api/orders/${code}/pay`, { method: "POST" });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    refreshStatus.textContent = data.error || "Nao foi possivel registrar o pagamento";
    return;
  }
  await refreshDashboard();
}

async function updateInventoryRequest(itemId, payload) {
  const response = await fetch(`/api/logistics/inventory/${itemId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    refreshStatus.textContent = data.error || "Falha na logistica";
    return;
  }

  updateLogistics(await response.json());
}

async function closeNote(noteId) {
  const response = await fetch(`/api/logistics/notes/${noteId}/close`, { method: "POST" });
  if (!response.ok) {
    refreshStatus.textContent = "Falha na nota";
    return;
  }
  updateLogistics(await response.json());
}

function renderCloseoutReport(report) {
  const mostOrdered = report.bebida_mais_pedida
    ? `${report.bebida_mais_pedida.name} (${report.bebida_mais_pedida.quantity}x)`
    : "Sem dados";
  const peakHour = report.pico_atendimento
    ? `${report.pico_atendimento.hour} (${report.pico_atendimento.orders} pedidos)`
    : "Sem dados";

  closeoutContent.innerHTML = [
    ["Total revenue", brl.format(report.total_recebido ?? report.total_vendido)],
    ["Total number of orders", String(report.total_pedidos)],
    ["Most ordered items", mostOrdered],
    ["Peak time", peakHour],
    ["Itens vendidos", String(report.total_itens_vendidos)],
    ["Custo total", brl.format(report.custo_total)],
    ["Lucro estimado", brl.format(report.lucro_estimado)],
  ]
    .map(
      ([label, value]) => `
      <div class="summary-item">
        <div><strong>${label}</strong></div>
        <strong>${value}</strong>
      </div>
    `
    )
    .join("");

  closeoutDrinks.innerHTML = report.quantidade_por_bebida.length
    ? report.quantidade_por_bebida
        .map(
          (item) => `
          <div class="summary-item">
            <div><strong>${item.name}</strong></div>
            <strong>${item.quantity}x</strong>
          </div>
        `
        )
        .join("")
    : '<p class="empty-inline">Nenhuma bebida vendida ainda.</p>';

  closeoutModal.classList.remove("hidden");
  closeoutModal.setAttribute("aria-hidden", "false");
}

function applyDashboardSnapshot(data) {
  if (Number.isInteger(Number(data.current_shift_id))) {
    currentShiftId = Number(data.current_shift_id);
  }
  refreshStatus.textContent = `Atualizado ${data.generated_at}`;
  updateSummary(data.summary);
  renderAwaitingPayment(data.awaiting_payment || []);
  renderPending(data.pending);
  renderCompleted(data.completed);
  renderTopItems(data.summary.top_items);
  renderTopTables(data.summary.top_tables);
  updateLogistics(data.logistics);
  renderShiftHistory(data.shifts || []);
}

async function closeBar() {
  if (closeoutRequestInFlight) {
    return;
  }
  closeoutRequestInFlight = true;
  setButtonBusy(closeBarButton, true, "Closing...", "Close Bar");
  try {
    const response = await fetch("/api/reports/closeout", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ expected_shift_id: currentShiftId }),
    });
    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      refreshStatus.textContent = data.error || "Falha ao fechar bar";
      return;
    }
    const data = await response.json();
    setCloseoutExportLink(data.closed_shift_id);
    renderCloseoutReport(data.report);
    applyDashboardSnapshot(data);
  } finally {
    closeoutRequestInFlight = false;
    setButtonBusy(closeBarButton, false, "Closing...", "Close Bar");
  }
}

async function resetData() {
  const confirmed = window.confirm("Isso vai encerrar o turno atual e abrir um novo painel vazio, mantendo o historico. Deseja continuar?");
  if (!confirmed) {
    return;
  }
  if (resetRequestInFlight) {
    return;
  }
  resetRequestInFlight = true;
  setButtonBusy(resetDataButton, true, "Resetando...", "Resetar dados");
  try {
    const response = await fetch("/api/reports/reset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ expected_shift_id: currentShiftId }),
    });
    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      refreshStatus.textContent = data.error || "Falha ao resetar";
      return;
    }
    if (!closeoutModal.classList.contains("hidden")) {
      closeoutModal.classList.add("hidden");
      closeoutModal.setAttribute("aria-hidden", "true");
    }
    const data = await response.json();
    setCloseoutExportLink(data.closed_shift_id);
    applyDashboardSnapshot(data);
  } finally {
    resetRequestInFlight = false;
    setButtonBusy(resetDataButton, false, "Resetando...", "Resetar dados");
  }
}

pendingContainer?.addEventListener("click", (event) => {
  const payButton = event.target.closest("[data-pay]");
  if (payButton) {
    payOrder(payButton.dataset.pay);
    return;
  }
  const button = event.target.closest("[data-complete]");
  if (button) {
    completeOrder(button.dataset.complete);
  }
});

awaitingPaymentContainer?.addEventListener("click", (event) => {
  const payButton = event.target.closest("[data-pay]");
  if (payButton) {
    payOrder(payButton.dataset.pay);
  }
});

completedContainer?.addEventListener("click", (event) => {
  const payButton = event.target.closest("[data-pay]");
  if (payButton) {
    payOrder(payButton.dataset.pay);
  }
});

inventoryContainer?.addEventListener("click", (event) => {
  const statusButton = event.target.closest("[data-status]");
  if (statusButton) {
    const row = statusButton.closest("[data-item-id]");
    updateInventoryRequest(row.dataset.itemId, { status: statusButton.dataset.status });
    return;
  }

  const stockActionButton = event.target.closest("[data-stock-action]");
  if (stockActionButton) {
    const row = stockActionButton.closest("[data-item-id]");
    const amountInput = row.querySelector(".inventory-amount-input");
    updateInventoryRequest(row.dataset.itemId, {
      stock_action: stockActionButton.dataset.stockAction,
      amount: amountInput?.value,
    });
    return;
  }

  const saveParButton = event.target.closest("[data-save-par]");
  if (saveParButton) {
    const row = saveParButton.closest("[data-item-id]");
    const parInput = row.querySelector(".inventory-par-input");
    updateInventoryRequest(row.dataset.itemId, {
      par_level: parInput?.value,
    });
  }
});

notesContainer?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-close-note]");
  if (button) {
    closeNote(button.dataset.closeNote);
  }
});

closeBarButton?.addEventListener("click", closeBar);
resetDataButton?.addEventListener("click", resetData);
closeCloseoutModalButton?.addEventListener("click", closeCloseoutModal);
closeCloseoutModalTopButton?.addEventListener("click", closeCloseoutModal);
closeoutModal?.addEventListener("click", (event) => {
  if (event.target === closeoutModal) {
    closeCloseoutModal();
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !closeoutModal?.classList.contains("hidden")) {
    closeCloseoutModal();
  }
});

setCloseoutExportLink(null);
refreshDashboard();
setInterval(refreshDashboard, 5000);
