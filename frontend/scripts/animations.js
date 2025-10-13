

// FAQ Toggle
const faqQuestions = document.querySelectorAll('.faq-question');

faqQuestions.forEach(question => {
  question.addEventListener('click', () => {
    const faqItem = question.parentElement;
    faqItem.classList.toggle('active');
  });
});

// Copy Pix Key
// Função para copiar a Base URL
document.getElementById('baseEndpoint').addEventListener('click', function() {
  const button = this;
  const baseUrl = 'https://api-buscacnpjsimples.com.br/cnpj/:cnpj';
  
  // Copiar para área de transferência
  navigator.clipboard.writeText(baseUrl).then(() => {
    // Salvar texto original
    const originalText = button.innerHTML;
    
    // Adicionar classe de sucesso
    button.classList.add('copied');
    
    // Mudar texto do botão
    button.innerHTML = '✓ Copiado!';
    
    // Remover classe e restaurar texto após 2 segundos
    setTimeout(() => {
      button.classList.remove('copied');
      button.innerHTML = originalText;
    }, 2000);
  }).catch(err => {
    console.error('Erro ao copiar:', err);
    button.innerHTML = '✗ Erro';
    setTimeout(() => {
      button.innerHTML = 'Copiar';
    }, 2000);
  });
});

// Função para copiar chave PIX (já existente no seu código)
function copyPixKey() {
  const pixKey = 'sua-chave-pix-aqui'; // Substitua pela sua chave PIX real
  const pixMessage = document.getElementById('pixMessage');
  
  navigator.clipboard.writeText(pixKey).then(() => {
    pixMessage.textContent = 'Chave Pix copiada com sucesso!';
    pixMessage.style.opacity = '1';
    
    setTimeout(() => {
      pixMessage.style.opacity = '0';
    }, 3000);
  }).catch(err => {
    console.error('Erro ao copiar chave Pix:', err);
    pixMessage.textContent = '✗ Erro ao copiar. Tente novamente.';
    pixMessage.style.color = '#ef4444';
    pixMessage.style.opacity = '1';
    
    setTimeout(() => {
      pixMessage.style.opacity = '0';
      pixMessage.style.color = '#009866';
    }, 3000);
  });
}


function getCurrentYear() {
  const year = new Date()
  year.getFullYear()
  return year
}

// Funções do Modal CNPJ
// Declarar variáveis globalmente
let modal;
let modalContent;
let testForm;

// Inicializar quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', function() {
  modal = document.getElementById('modalCnpj');
  modalContent = document.getElementById('modalContent');
  testForm = document.getElementById('testForm');
  
  initializeModal();
});

// Abrir modal
function openModal() {
  if (modal) {
    modal.classList.add('active');
    document.body.style.overflow = 'hidden';
  }
}

// Fechar modal
function closeModal() {
  if (modal) {
    modal.classList.remove('active');
    document.body.style.overflow = '';
  }
}

// Inicializar eventos do modal
function initializeModal() {
  // Fechar modal ao clicar fora
  if (modal) {
    modal.addEventListener('click', (e) => {
      if (e.target === modal) {
        closeModal();
      }
    });
  }

  // Fechar modal com tecla ESC
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && modal && modal.classList.contains('active')) {
      closeModal();
    }
  });

  // Adicionar evento de submit ao formulário
  if (testForm) {
    testForm.addEventListener('submit', handleFormSubmit);
  }
}

// Formatar CNPJ para exibição
function formatCnpj(cnpj) {
  return cnpj.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/, '$1.$2.$3/$4-$5');
}

// Formatar data
function formatDate(dateString) {
  if (!dateString) return 'Não informado';
  const [year, month, day] = dateString.split('-');
  return `${day}/${month}/${year}`;
}

// Renderizar dados no modal
function renderModalData(data) {
  if (!modalContent) return;
  
  const situacao = data.situacao_cadastral?.toLowerCase() === 'ativa' ? 'active' : 'inactive';
  const situacaoIcon = situacao === 'active' 
    ? '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>'
    : '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="15" y1="9" x2="9" y2="15"></line><line x1="9" y1="9" x2="15" y2="15"></line></svg>';

  modalContent.innerHTML = `
    <div class="status-badge ${situacao}">
      ${situacaoIcon}
      ${data.situacao_cadastral || 'Status não informado'}
    </div>

    <div class="modal-data-grid">
      <div class="data-item">
        <div class="data-label">CNPJ</div>
        <div class="data-value">${formatCnpj(data.cnpj)}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Razão Social</div>
        <div class="data-value">${data.razao_social || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Nome Fantasia</div>
        <div class="data-value">${data.nome_fantasia || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Data de Abertura</div>
        <div class="data-value">${formatDate(data.data_abertura)}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Natureza Jurídica</div>
        <div class="data-value">${data.natureza_juridica || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Porte</div>
        <div class="data-value">${data.porte || '<span class="empty">Não informado</span>'}</div>
      </div>
    </div>

    <div class="modal-divider"></div>

    <div class="modal-data-grid">
      <div class="data-item">
        <div class="data-label">Logradouro</div>
        <div class="data-value">${data.logradouro || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Número</div>
        <div class="data-value">${data.numero || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Complemento</div>
        <div class="data-value">${data.complemento || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Bairro</div>
        <div class="data-value">${data.bairro || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Município</div>
        <div class="data-value">${data.municipio || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">UF</div>
        <div class="data-value">${data.uf || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">CEP</div>
        <div class="data-value">${data.cep || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Email</div>
        <div class="data-value">${data.email || '<span class="empty">Não informado</span>'}</div>
      </div>

      <div class="data-item">
        <div class="data-label">Telefone</div>
        <div class="data-value">${data.telefone || '<span class="empty">Não informado</span>'}</div>
      </div>
    </div>

    <div class="modal-footer">
      <button class="modal-button modal-button-secondary" onclick="closeModal()">
        Fechar
      </button>
      <button class="modal-button modal-button-primary" onclick="copyModalData()">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
          <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
        </svg>
        Copiar Dados
      </button>
    </div>
  `;
}

// Renderizar erro no modal
function renderModalError(message) {
  if (!modalContent) return;
  
  modalContent.innerHTML = `
    <div class="modal-error">
      <div class="modal-error-icon">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="12" cy="12" r="10"></circle>
          <line x1="12" y1="8" x2="12" y2="12"></line>
          <line x1="12" y1="16" x2="12.01" y2="16"></line>
        </svg>
      </div>
      <p class="modal-error-text">${message}</p>
    </div>
    <div class="modal-footer">
      <button class="modal-button modal-button-secondary" onclick="closeModal()">
        Fechar
      </button>
    </div>
  `;
}

// Copiar dados do modal
function copyModalData() {
  if (!modalContent) return;
  
  const dataItems = modalContent.querySelectorAll('.data-item');
  let text = 'DADOS DA EMPRESA\n\n';
  
  dataItems.forEach(item => {
    const label = item.querySelector('.data-label').textContent;
    const value = item.querySelector('.data-value').textContent.trim();
    if (value && !value.includes('Não informado')) {
      text += `${label}: ${value}\n`;
    }
  });

  navigator.clipboard.writeText(text).then(() => {
    const btn = event.target.closest('button');
    const originalText = btn.innerHTML;
    btn.innerHTML = `
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="20 6 9 17 4 12"></polyline>
      </svg>
      Copiado!
    `;
    setTimeout(() => {
      btn.innerHTML = originalText;
    }, 2000);
  });
}

// Submissão do formulário
async function handleFormSubmit(e) {
  e.preventDefault();
  
  const cnpjInput = document.getElementById('cnpjInput');
  const cnpj = cnpjInput.value.replace(/\D/g, '');
  
  if (cnpj.length !== 14) {
    alert('Por favor, insira um CNPJ válido com 14 dígitos.');
    return;
  }

  // Abrir modal com loading
  openModal();
  if (modalContent) {
    modalContent.innerHTML = `
      <div class="modal-loading">
        <div class="spinner"></div>
        <p class="modal-loading-text">Consultando CNPJ...</p>
      </div>
    `;
  }

  try {
    const response = await fetch(`https://api-buscacnpjsimples.com.br/cnpj/${cnpj}`);
    
    if (!response.ok) {
      throw new Error('CNPJ não encontrado ou erro na consulta.');
    }
    
    const data = await response.json();
    renderModalData(data);
    
  } catch (error) {
    renderModalError(
      error.message || 'Erro ao consultar o CNPJ. Por favor, tente novamente.'
    );
  }
}