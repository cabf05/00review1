# Coletor de Reviews (Streamlit)

Aplicação Streamlit para **processar e filtrar** reviews do Google Maps no deploy do Streamlit Cloud, **sem uso de serviços pagos**.

## Como funciona

1. Você informa a URL do local no Google Maps (para validação/referência).
2. Você define o período em `Últimos X dias` (inteiro positivo, padrão `2000`, mínimo `1`).
3. Você envia um arquivo `.json` ou `.csv` com os reviews.
4. O app normaliza as colunas e filtra pelo período (`utc_now - timedelta(days=X)`) usando `publishedAtDate` (ou equivalente), exibindo total coletado e total após filtro.

## Pré-requisitos

- Python 3.10+
- `pip`

## Instalação de dependências

```bash
pip install -r requirements.txt
```

## Playwright: instalação dos browsers necessários

Após instalar as dependências Python, instale também os browsers usados pelo Playwright:

```bash
python -m playwright install chromium
```

Se necessário para o ambiente, você pode instalar todos os browsers suportados:

```bash
python -m playwright install
```

## Limitações operacionais

- A automação com browser tende a ter **tempo de processamento maior** que abordagens sem navegação.
- Plataformas de terceiros podem aplicar **bloqueios temporários** (rate limit, captcha ou respostas intermitentes).
- Ambientes de nuvem com recursos limitados podem apresentar variação de performance.
- O scraper usa `playwright-stealth` para reduzir bloqueios por detecção de automação, mas ainda pode haver captcha em picos de tráfego.

## Recomendações de uso

- Execute coletas em **janelas de tempo maiores** para reduzir a frequência de chamadas.
- Implemente **retries com backoff** para falhas transitórias.
- Evite paralelismo agressivo para diminuir chance de bloqueio.
- Monitore logs e trate timeouts de forma explícita.

## Streamlit Cloud (deploy)

- Configure o app com `app.py` como arquivo principal.
- Como o projeto usa Playwright, garanta build steps compatíveis para instalar browsers no deploy.
- Exemplo de etapa de build/pós-instalação no Streamlit Cloud:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

> Dependendo da imagem/base do ambiente, pode ser necessário complementar com bibliotecas de sistema exigidas pelo Playwright.

## Formato esperado do arquivo

Campos recomendados (aceita aliases comuns):
- `title`
- `name`
- `text`
- `publishedAtDate` (obrigatório para entrar no filtro de período)
- `stars`
- `likesCount`
- `reviewUrl`
- `responseFromOwnerText`

## Exemplo de URL válida

- `https://www.google.com/maps/place/Nome+do+Local/@-23.5505,-46.6333,17z`
- `https://maps.app.goo.gl/abc123xyz`

## Estrutura

- `app.py`: interface Streamlit para upload/processamento.
- `src/reviews_service.py`: valida URL, leitura JSON/CSV, normalização e filtro por data absoluta.
- `requirements.txt`: dependências do projeto.
- `README.md`: instruções de uso.
