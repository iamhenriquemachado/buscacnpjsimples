

// FAQ Toggle
const faqQuestions = document.querySelectorAll('.faq-question');

faqQuestions.forEach(question => {
  question.addEventListener('click', () => {
    const faqItem = question.parentElement;
    faqItem.classList.toggle('active');
  });
});

// Copy Pix Key
function copyPixKey() {
  const pixKey = 'sua-chave-pix-aqui';
  navigator.clipboard.writeText(pixKey).then(() => {
    const message = document.getElementById('pixMessage');
    message.textContent = '✓ Chave Pix copiada!';
    setTimeout(() => {
      message.textContent = '';
    }, 3000);
  });
}

// Form submission
document.getElementById('testForm').addEventListener('submit', (e) => {
  e.preventDefault();
  const cnpj = document.getElementById('cnpjInput').value;
  alert(`Consultando CNPJ: ${cnpj}`);
});