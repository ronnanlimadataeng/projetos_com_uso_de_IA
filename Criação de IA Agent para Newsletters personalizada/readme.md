# Automação de Coleta, Processamento e Envio de Resultados por E-mail

Este projeto implementa um fluxo automatizado para coletar conteúdos de URLs, processar informações, extrair textos relevantes com LLM e enviar os resultados processados por e-mail para uma lista de destinatários.

---

## Fluxo Geral
<img width="1793" height="336" alt="image" src="https://github.com/user-attachments/assets/1a38917e-a108-4d76-ab6d-15065513da7f" />

1. **Agendamento**  
   Define o horário em que a execução será disparada automaticamente.

2. **Carregar URLs**  
   Lê a planilha **“Urls de sites”**, que contém a lista de páginas a serem processadas.

3. **Controle de Lotes (opcional)**  
   Se houver mais de 10 URLs, o processo pode ser dividido em lotes.  
   O bloco `Wait` pode pausar entre lotes (atualmente desativado).

---

## Processamento por URL

Para cada URL carregada:

1. **Requisição HTTP** – Busca a página.  
2. **Extração do HTML** – Obtém o conteúdo bruto da página.  
3. **Limpeza do Conteúdo** – Remove tags e ruídos desnecessários.  
4. **Extração de Texto Relevante (LLM)** – Um modelo de linguagem captura apenas o conteúdo útil do HTML.  
5. **Parse JSON** – Estrutura a saída no formato JSON.

---

## Pós-Processamento

- **Aggregate** – Agrega os resultados de todas as URLs em um único conjunto de dados.  
- **Agent – Pesquisar** – Um agente (chat model + memória + ferramentas) analisa os dados agregados e produz o resultado final.  

---

## Envio de Resultados

1. **Carregar Lista de E-mails**  
   Lê a planilha **“Lista de Emails2”** com os destinatários.  

2. **Enviar E-mail**  
   Dispara mensagens com os resultados processados.  

---

## Estrutura do Projeto

- **Planilhas**  
  - `Urls de sites` → lista de URLs a processar.  
  - `Lista de Emails2` → destinatários do relatório final.  

- **Blocos Principais**  
  - Agendamento  
  - Requisição HTTP  
  - Extração e Limpeza de HTML  
  - LLM para captura de texto  
  - Agregação de resultados  
  - Análise com Agente (Chat Model)  
  - Envio de e-mails  

---

## Como Usar

1. Preencher a planilha `Urls de sites` com os links desejados.  
2. Configurar os destinatários na planilha `Lista de Emails2`.  
3. Definir o horário de execução no bloco **Agendamento**.  
4. Ativar a automação para rodar de forma recorrente.  

---

## Observações

- O controle de lotes está desativado, mas pode ser habilitado caso haja grande volume de URLs.
- O fluxo utiliza um modelo de linguagem (LLM) para garantir que apenas o conteúdo relevante seja extraído.  
- A etapa de envio de e-mails pode ser personalizada para incluir templates de corpo e assunto da mensagem.  

<img width="1776" height="463" alt="image" src="https://github.com/user-attachments/assets/da606bb4-d5d3-42d7-8494-eca8192f321c" />









