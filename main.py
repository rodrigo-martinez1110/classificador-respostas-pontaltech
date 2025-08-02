import pandas as pd
import streamlit as st
import unicodedata
import re
import io

palavras_nao_perturbe = [
    "nao me ligue", "nao quero receber", "me tira da lista", "descadastre", "vou processar", "perturb", 
    "enche o saco", "nao autorizo", "privacidade", "cancelar", "retire meu numero", 'nao me mand'
    "bloquear", "vou denunciar", "procon", "denuncia", "vai se fu", "fodase", 'incomod', 'nao me envi',
    "vtnc", "pqp", "krl", "desgraca", "inferno", "dados", 'cancele', "cancelada", 'justica', 'processo', 'judicial', 'pelo amor de deus', 'para de mandar',
    'toma no cu', 'sacanagem', 'porcaria', 'caralho', 'desgraca', 'disgraca', 'disgrama', 'para de envia', 'parem de me', 'cancela',
    "vai tomar no cu", "encher o saco", "meu saco", "nao perturbe", "nao me perturbe", "nmp", 'bloquear', 'canselar'
]

wrong_recipient_messages = [
    'presente gratis', "conheco", "morreu", "faleceu", 'nao sou', 'faleceu', 'meu nome', 'nao e de nenhum', 'nao e', 'meu numero', 'esse numero n'
]

sem_interesse = [
    "sem interesse", 'nem escrevi', 'nem inscrevi', 'nao inscrev', 'como voces', "nao quero", "nao tenho interesse", "nao estou interessado", 'nao estou',
    "nao funciona", "nao preciso", "nao ajuda", "nao estou afim", "nao quero contratar", 'parar', 'nada', "sair", 'nao', "para de encher"
]

interesse = ['falar com especialista', 'comecar', 'ok', 'quanto', 'qual a margem', 'margem', 'saber mais', 'saber detalhes', 'detalhes', 'detalhar', 'sim', 'simule', 'quero', 'simulacao', 'qual a taxa', 'taxa', 'informe a taxa', 'como pego', 'como faco', 'interessado',
             'me explique', 'beleza', 'como sacar', 'quero', 'qual banco', 'especialista', 'quanto de juros', 'quero', 'sem margem', 'tenho interesse', 'enteresi', 'enteressi', 'interessado', 'interesse']

def limpar_texto(texto):
    texto = str(texto).lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def classificar_resposta(texto):
    texto = limpar_texto(texto)

    if any(p in texto for p in palavras_nao_perturbe):
        return "nmp"
    elif any(p in texto for p in wrong_recipient_messages):
        return "wrong_recipient"
    elif any(p in texto for p in sem_interesse):
        return "Sem Interesse"
    elif any(p in texto for p in interesse):
        return "Interessado"
    else:
        return "Outros"

def processar_em_chunks(uploaded_file, coluna_resposta, chunk_size=100_000):
    resultados = []
    total_linhas = 0
    progresso = st.progress(0)
    contador_chunks = 0

    for chunk in pd.read_csv(uploaded_file, sep=';', chunksize=chunk_size, low_memory=False):
        chunk = chunk[chunk['STATUS'] == 'RESPONDIDO']
        if coluna_resposta in chunk.columns:
            chunk["CLASSIFICACAO"] = chunk[coluna_resposta].apply(classificar_resposta)
            resultados.append(chunk[[coluna_resposta, "CLASSIFICACAO", 'VAR2', 'NUMERO']])
        total_linhas += len(chunk)
        contador_chunks += 1
        progresso.progress(min(contador_chunks * 0.1, 1.0))  # Estimativa simples

    progresso.empty()
    if resultados:
        return pd.concat(resultados, ignore_index=True)
    else:
        return pd.DataFrame()

# Streamlit App
st.title("Classificador de Respostas de Clientes")
st.markdown("Otimizador para grandes volumes de dados (até 1 GB por arquivo)")

arquivos = st.file_uploader("Carregue os arquivos CSV", type="csv", accept_multiple_files=True)

coluna_resposta = 'MENSAGEM'

if arquivos:
    resultados_gerais = []

    for arquivo in arquivos:
        st.subheader(f"Processando: {arquivo.name}")
        try:
            resultado = processar_em_chunks(arquivo, coluna_resposta)

            resultado = resultado[['NUMERO', 'MENSAGEM', 'VAR2', 'CLASSIFICACAO']]
            resultado.columns = ['contato', 'mensagem', 'cpf', 'classificacao']
            resultado = resultado[['cpf', 'classificacao', 'contato', 'mensagem']]

            if not resultado.empty:
                st.success(f"Classificação finalizada para {arquivo.name}")
                st.dataframe(resultado.head(5))
                resultados_gerais.append(resultado)
            else:
                st.warning(f"Nenhum dado válido encontrado em {arquivo.name}.")
        except Exception as e:
            st.error(f"Erro ao processar {arquivo.name}: {e}")

    if resultados_gerais:
        resultado_final = pd.concat(resultados_gerais, ignore_index=True)
        csv_resultado = resultado_final.to_csv(index=False, sep=';').encode("utf-8")
        st.download_button(
            "📥 Baixar todas as classificações combinadas",
            csv_resultado,
            file_name="classificacoes_agregadas.csv",
            mime="text/csv"
        )


