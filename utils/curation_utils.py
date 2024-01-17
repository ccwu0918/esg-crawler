from sentence_transformers import SentenceTransformer


def to_embeddings(text_list: list):
    embedding_model_name = "shibing624/text2vec-base-chinese"
    embedding_model = SentenceTransformer(embedding_model_name, device="cpu")
    embeddings = embedding_model.encode(text_list)
    print(embeddings.shape)
    return embeddings.tolist()
