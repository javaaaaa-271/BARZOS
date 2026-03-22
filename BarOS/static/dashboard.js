const pendingContainer = document.getElementById("pending-orders");
const completedContainer = document.getElementById("completed-orders");
const topItemsContainer = document.getElementById("top-items");
const topTablesContainer = document.getElementById("top-tables");
const inventoryContainer = document.getElementById("inventory-items");
const notesContainer = document.getElementById("shift-notes");
const refreshStatus = document.getElementById("refresh-status");
const closeBarButton = document.getElementById("close-bar-button");
const resetDataButton = document.getElementById("reset-data-button");
const closeoutModal = document.getElementById("closeout-modal");
const closeoutContent = document.getElementById("closeout-content");
const closeoutDrinks = document.getElementById("closeout-drinks");
const closeCloseoutModalButton = document.getElementById("close-closeout-modal");

const brl = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
});

function renderOrderItems(items) {
  return items
    .map((item) => `<li>${item.quantity}x ${item.name} <strong>${brl.format(item.subtotal)}</strong></li>`)
    .join("");
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
          <strong>${order.code}</strong>
          <span class="pill">${order.created_at}</span>
        </div>
        <h3>Pedido em aberto</h3>
        <p>Retirada no balcao</p>
        <ul>${renderOrderItems(order.items)}</ul>
        <div class="order-actions">
          <span class="muted-note">Total ${brl.format(order.total)}</span>
          <button class="primary-button" data-complete="${order.code}" type="button">Marcar como entregue</button>
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
          <strong>${order.code}</strong>
          <span class="status-badge">Concluido</span>
        </div>
        <h3>Pedido finalizado</h3>
        <p>Registrado em ${order.created_at}</p>
        <ul>${renderOrderItems(order.items)}</ul>
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
    topTablesContainer.innerHTML = '<p class="empty-inline">Sem agrupamento adicional no MVP atual.</p>';
    return;
  }

  topTablesContainer.innerHTML = tables
    .map((table) => `<div class="insight-row"><span>${table.table_label}</span><strong>${table.total}</strong></div>`)
    .join("");
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
        <div>
          <strong>${item.name}</strong>
          <p>${item.category} · ${item.stock_level} ${item.unit} / minimo ${item.par_level}</p>
        </div>
        <div class="inventory-actions">
          <span class="status-pill ${item.status}">${item.status}</span>
          <button class="ghost-button small-button" type="button" data-status="ok">OK</button>
          <button class="ghost-button small-button" type="button" data-status="attention">Atencao</button>
          <button class="ghost-button small-button" type="button" data-status="critical">Critico</button>
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

function updateSummary(summary) {
  document.getElementById("pending-count").textContent = summary.pending_count;
  document.getElementById("completed-count").textContent = summary.completed_count;
  document.getElementById("total-count").textContent = summary.total_count;
  document.getElementById("revenue-total").textContent = brl.format(summary.revenue);
  document.getElementById("average-ticket").textContent = brl.format(summary.average_ticket);
}

function updateLogistics(logistics) {
  document.getElementById("critical-count").textContent = logistics.inventory_summary.critical_count;
  document.getElementById("attention-count").textContent = logistics.inventory_summary.attention_count;
  document.getElementById("tracked-count").textContent = logistics.inventory_summary.tracked_count;
  renderInventory(logistics.inventory);
  renderNotes(logistics.notes);
}

async function refreshDashboard() {
  const response = await fetch("/api/orders");
  if (!response.ok) {
    refreshStatus.textContent = "Falha ao atualizar";
    return;
  }

  const data = await response.json();
  refreshStatus.textContent = `Atualizado ${data.generated_at}`;
  updateSummary(data.summary);
  renderPending(data.pending);
  renderCompleted(data.completed);
  renderTopItems(data.summary.top_items);
  renderTopTables(data.summary.top_tables);
  updateLogistics(data.logistics);
}

async function completeOrder(code) {
  const response = await fetch(`/api/orders/${code}/complete`, { method: "POST" });
  if (!response.ok) {
    refreshStatus.textContent = "Nao foi possivel concluir";
    return;
  }
  await refreshDashboard();
}

async function updateInventoryStatus(itemId, status) {
  const response = await fetch(`/api/logistics/inventory/${itemId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status }),
  });
  if (!response.ok) {
    refreshStatus.textContent = "Falha na logistica";
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
    ["Total vendido", brl.format(report.total_vendido)],
    ["Total de pedidos", String(report.total_pedidos)],
    ["Itens vendidos", String(report.total_itens_vendidos)],
    ["Bebida mais pedida", mostOrdered],
    ["Pico de atendimento", peakHour],
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
  refreshStatus.textContent = `Atualizado ${data.generated_at}`;
  updateSummary(data.summary);
  renderPending(data.pending);
  renderCompleted(data.completed);
  renderTopItems(data.summary.top_items);
  renderTopTables(data.summary.top_tables);
  updateLogistics(data.logistics);
}

async function closeBar() {
  const response = await fetch("/api/reports/closeout", { method: "POST" });
  if (!response.ok) {
    refreshStatus.textContent = "Falha ao fechar bar";
    return;
  }
  const data = await response.json();
  renderCloseoutReport(data.report);
  applyDashboardSnapshot(data);
}

async function resetData() {
  const confirmed = window.confirm("Isso vai encerrar o turno atual e abrir um novo painel vazio, mantendo o historico. Deseja continuar?");
  if (!confirmed) {
    return;
  }
  const response = await fetch("/api/reports/reset", { method: "POST" });
  if (!response.ok) {
    refreshStatus.textContent = "Falha ao resetar";
    return;
  }
  if (!closeoutModal.classList.contains("hidden")) {
    closeoutModal.classList.add("hidden");
    closeoutModal.setAttribute("aria-hidden", "true");
  }
  applyDashboardSnapshot(await response.json());
}

pendingContainer?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-complete]");
  if (button) {
    completeOrder(button.dataset.complete);
  }
});

inventoryContainer?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-status]");
  if (button) {
    const row = button.closest("[data-item-id]");
    updateInventoryStatus(row.dataset.itemId, button.dataset.status);
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
closeCloseoutModalButton?.addEventListener("click", () => {
  closeoutModal.classList.add("hidden");
  closeoutModal.setAttribute("aria-hidden", "true");
});

refreshDashboard();
setInterval(refreshDashboard, 5000);
