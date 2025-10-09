

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

// Form submission
document.getElementById('testForm').addEventListener('submit', (e) => {
  e.preventDefault();
  const cnpj = document.getElementById('cnpjInput').value;
  alert(`Consultando CNPJ: ${cnpj}`);
});