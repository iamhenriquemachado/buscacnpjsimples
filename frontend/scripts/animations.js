document.querySelectorAll(".faq-question").forEach((btn) => {
        btn.addEventListener("click", () => {
          const item = btn.parentElement;
          item.classList.toggle("active");
        });
      });


// Teste da API
      const testForm = document.getElementById("testForm");
      const cnpjInput = document.getElementById("cnpjInput");
      const modalOverlay = document.getElementById("modalOverlay");
      const modalClose = document.getElementById("modalClose");
      const modalBody = document.getElementById("modalBody");

      // Permitir apenas números no input
      cnpjInput.addEventListener("input", (e) => {
        e.target.value = e.target.value.replace(/\D/g, "");
      });

      testForm.addEventListener("submit", async (e) => {
        e.preventDefault();
        const cnpj = cnpjInput.value.trim();

        if (cnpj.length !== 14) {
          showModal(
            '<div class="error-message">Por favor, digite um CNPJ válido com 14 dígitos.</div>'
          );
          return;
        }

        const button = testForm.querySelector("button");
        button.disabled = true;
        button.textContent = "Consultando...";

        try {
          const response = await fetch(`https://suaapi.com.br/cnpj/${cnpj}`);
          const data = await response.json();

          if (response.ok) {
            const formattedJson = JSON.stringify(data, null, 2);
            const highlightedJson = highlightJson(formattedJson);
            showModal(`<pre>${highlightedJson}</pre>`);
          } else {
            showModal(
              `<div class="error-message">Erro: ${
                data.message || "CNPJ não encontrado"
              }</div>`
            );
          }
        } catch (error) {
          showModal(
            '<div class="error-message">Erro ao consultar a API. Verifique sua conexão.</div>'
          );
        } finally {
          button.disabled = false;
          button.textContent = "Consultar";
        }
      });

      function highlightJson(json) {
        return json
          .replace(
            /"([^"]+)":/g,
            '<span class="json-key">"$1"</span><span class="json-punctuation">:</span>'
          )
          .replace(/: "([^"]+)"/g, ': <span class="json-string">"$1"</span>')
          .replace(/([,{}[\]])/g, '<span class="json-punctuation">$1</span>');
      }

      function showModal(content) {
        modalBody.innerHTML = content;
        modalOverlay.classList.add("active");
      }

      modalClose.addEventListener("click", () => {
        modalOverlay.classList.remove("active");
      });

      modalOverlay.addEventListener("click", (e) => {
        if (e.target === modalOverlay) {
          modalOverlay.classList.remove("active");
        }
      });