# 📊 Projeto Matriz AeC

[![Status do Projeto](https://img.shields.io/badge/Status-Em%20Uso-blue.svg)](README.md)
[![Desenvolvido em](https://img.shields.io/badge/Linguagem-Python%20%7C%20JS-blue)](https://www.python.org/)
[![Frameworks](https://img.shields.io/badge/Frameworks-FastAPI%20%7C%20HTMX-orange.svg)](https://fastapi.tiangolo.com/)

O Projeto Matriz é um sistema web desenvolvido com **Python (FastAPI)** e **HTMX** que visa automatizar e centralizar o processo de criação e validação das Matrizes de Distribuição. O objetivo principal é proporcionar maior autonomia às áreas da AeC, reduzindo a necessidade de reuniões presenciais para alinhamento e validação.

---

## 🎯 Público-Alvo

O sistema foi desenhado para uso estratégico pelas seguintes áreas da AeC:

* **Gerentes e Coordenadores de Operações**
* **Gerentes e Coordenadores de Qualidade**
* **Gerentes e Coordenadores de Planejamento**
* **Excelência Operacional (EXOP)**

---

## 🌟 Fluxo de Trabalho e Funcionalidades

O sistema foi projetado para seguir um fluxo claro de submissão e validação em três níveis, garantindo a integridade dos dados:

1.  **Cadastro Operacional:** Gerentes/Coordenadores de Operação iniciam o processo cadastrando as Matrizes operacionais.
2.  **Acordo das Áreas de Apoio:** Gerentes/Coordenadores de Qualidade e Planejamento revisam a matriz e registram Acordo ou Não Acordo.
3.  **Validação Final (EXOP):** A área de Excelência Operacional (EXOP) dá o Acordo final, baseando-se nas respostas das áreas de apoio e na integridade geral da Matriz cadastrada.

---

## 🛠️ Tecnologias e Desempenho

Este projeto foi construído sobre uma arquitetura moderna e performática:

* **Backend:** Python 3.10+ com **FastAPI** (serviço web de alta performance).
* **Frontend:** HTML e CSS, potencializados por **HTMX** para interações dinâmicas sem JavaScript complexo.
* **Persistência de Dados:** Todas as informações (Matriz_Geral e Acessos_Matriz) persistem em banco de dados, incluindo logs de submissão e validação.
* **Performance:**
    * Utilização de **assincronismo (Async/Await)** nas consultas de I/O em banco de dados.
    * Gerenciamento eficiente com **Pool de Conexões**, resultando em um desempenho suscetível para o público-alvo.
* **Segurança:** Senhas de usuários são criptografadas no banco de dados.

---

## 🚀 Como Utilizar (Uso Local)

Siga os passos abaixo para instalar e rodar o projeto localmente:

1.  **Instalar as Dependências:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Rodar Localmente (Desenvolvimento):**
    ```bash
    uvicorn main:app --reload
    ```
    O servidor estará disponível em `http://127.0.0.1:8000`.

3.  **Rodar em Produção (Acesso Externo):**
    Para permitir que outras máquinas acessem o sistema (via IP, Cloudflare ou Ngrok):
    ```bash
    # (nucleos * 2 + 1) é a fórmula sugerida para o número de workers
    uvicorn main:app --host 0.0.0.0 --port 8000 --workers (nucleos*2+1)
    ```

---

## ℹ️ Informações Adicionais

* **Início do Projeto:** 19 de Setembro.
* **Fase Atual:** Dev e testes.
* **Estrutura da Aplicação:** O sistema é dividido em 3 páginas principais, separando as funcionalidades por público-alvo: Operação, Áreas de Apoio e Administração (EXOP).
* **Validações:** O sistema lida de forma robusta com diversos tipos de validações de dados para garantir a integridade das informações inseridas pelos usuários.
* **Usuários:** Os usuários foram cadastrados de forma fictícia para fins de testes e demonstração.