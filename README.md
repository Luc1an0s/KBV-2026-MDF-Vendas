# KBV-2026-MDF-Vendas

Sistema de sincronização incremental de dados de vendas de MDF entre banco de dados MySQL e Google Sheets.

## 📋 Descrição

Este projeto extrai dados de vendas de um banco de dados MySQL através de uma conexão SSH segura e os envia para uma planilha Google Sheets. O sistema realiza sincronizações incrementais, processando apenas dados novos desde a última execução através de um arquivo de controle.

### Funcionalidades

- **Sincronização Incremental**: Rastreia o último registro processado para evitar duplicatas
- **Conexão SSH Segura**: Acessa o banco de dados MySQL através de túnel SSH
- **Integração Google Sheets**: Envia dados para planilhas automaticamente
- **Processamento de Dados**: Realiza conversões de unidades e formatação de datas
- **Tratamento de Erros**: Logs detalhados e tratamento de exceções

## 🔧 Pré-requisitos

- Python 3.7+
- Acesso SSH ao servidor com banco de dados MySQL
- Credenciais de conta Google com permissões para Google Sheets API
- Chave de conta de serviço do Google (arquivo JSON)

## 📦 Instalação

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/KBV-2026-MDF-Vendas.git
cd KBV-2026-MDF-Vendas
```

2. Crie um ambiente virtual:
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# ou
source venv/bin/activate  # Linux/Mac
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

## ⚙️ Configuração

### Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
# Configurações SSH
SSH_HOST=seu_host_ssh
SSH_PORT=22
SSH_USER=seu_usuario_ssh
SSH_PASSWORD=sua_senha_ssh

# Configurações do Banco de Dados MySQL
DB_HOST=localhost
DB_USER=usuario_mysql
DB_PASS=senha_mysql
DB_NAME=nome_banco
DB_PORT=3306

# Configurações do Google Sheets
SPREADSHEET_ID=seu_id_de_planilha
ABA_NOME=nome_da_aba_destino
```

### Credenciais Google

1. Crie uma conta de serviço no [Google Cloud Console](https://console.cloud.google.com/)
2. Baixe a chave JSON e salve como `credenciais_google.json` na raiz do projeto
3. Compartilhe a planilha com o email da conta de serviço

## 🚀 Uso

Execute o script principal:

```bash
python main.py
```

O sistema irá:
1. Verificar o último controle de execução
2. Conectar ao banco de dados via SSH
3. Buscar dados novos desde a última sincronização
4. Processar e formatar os dados
5. Enviar para a planilha Google Sheets
6. Atualizar o arquivo de controle

## 📁 Estrutura de Arquivos

- `main.py` - Script principal com toda a lógica de sincronização
- `requirements.txt` - Dependências do projeto
- `controle_incremental.json` - Arquivo de controle da última sincronização
- `credenciais_google.json` - Credenciais da conta de serviço Google
- `.env` - Variáveis de ambiente (não versionado)

## 🔍 Detalhes da Consulta

A consulta SQL extrai dados de vendas com as seguintes informações:

- Informações de timestamp e nota fiscal
- Dados do cliente (CPF/CNPJ, código interno, nome)
- Código do pedido
- Informações do produto (SKU, descrição, categoria)
- Quantidade e valor unitário

Filtros aplicados:
- Apenas loja 1
- Apenas produtos da categoria 5 (MDF)
- Registros posteriores ao último controle

## 📊 Fluxo de Dados

```
MySQL (SSH) → Processamento → Google Sheets
     ↓
Atualiza controle_incremental.json
```

## 🐛 Tratamento de Erros

O sistema fornece mensagens detalhadas para:
- Falhas de conexão SSH/MySQL
- Erros de autenticação no Google Sheets
- Problemas com credenciais

## 📝 Logging

Mensagens informativas são exibidas durante a execução:
- Tentativas de conexão
- Quantidade de linhas processadas
- Status final de cada sincronização

## 🔒 Segurança

- Credenciais armazenadas em variáveis de ambiente e arquivo `.env`
- Conexão SSH criptografada
- Arquivo de credenciais Google não versionado
- Arquivo `.env` não versionado

## 📄 Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 👥 Contribuindo

Contribuições são bem-vindas! Por favor:

1. Faça um Fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## ❓ Suporte

Para dúvidas ou problemas, abra uma issue no repositório.

---

**Desenvolvido em 2026**