// Dark Mode Toggle
const themeToggle = document.getElementById('themeToggle');
const themeIcon = document.getElementById('themeIcon');
const html = document.documentElement;

// Verifica preferência salva ou do sistema
const savedTheme = localStorage.getItem('theme');
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

if (savedTheme === 'dark' || (!savedTheme && prefersDark)) {
  html.setAttribute('data-theme', 'dark');
  themeIcon.textContent = '☀️';
}

themeToggle.addEventListener('click', () => {
  const currentTheme = html.getAttribute('data-theme');
  const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
  
  html.setAttribute('data-theme', newTheme);
  localStorage.setItem('theme', newTheme);
  themeIcon.textContent = newTheme === 'dark' ? '☀️' : '🌙';
});

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