from sentence_transformers import SentenceTransformer
from openai import OpenAI
import numpy as np
client = OpenAI()

def split_text(text, max_length):
    """將長文本切割為多個最大長度的子串"""
    return [text[i:i+max_length] for i in range(0, len(text), max_length)]

def to_embeddings(text_list: list):
    embedding_model_name = "shibing624/text2vec-base-chinese"
    embedding_model = SentenceTransformer(embedding_model_name, device="cpu")
    embeddings = embedding_model.encode(text_list)
    print(embeddings.shape)
    return embeddings.tolist()

def to_openai_embeddings(text_list: list, max_length=2048):
    embeddings_list = []
    for text in text_list:
        # 檢查文本長度，如果超過最大長度則進行切割
        if len(text) > max_length:
            text_parts = split_text(text, max_length)
        else:
            text_parts = [text]

        part_embeddings = []
        for part in text_parts:
            # the old version, e.g. `pip install openai==0.28`
            # no longer supported in openai>=1.0.0

            # response = openai.Embedding.create(
            #     input=[part],
            #     model="text-embedding-3-small"
            # )
            # response = openai.Engine("text-embedding-3-small").embeddings(
            #     input=[part]
            # )
            # embeddings = response['data'][0]['embedding']
                        
            response = client.embeddings.create(
                input=[part],
                model="text-embedding-3-small"
            )            
            embeddings = response.data[0].embedding
            part_embeddings.append(embeddings)
        
        # 將同一文本的多部分嵌入向量平均處理以獲取整體嵌入向量
        embeddings_list.append(np.mean(part_embeddings, axis=0).tolist())

    # 將嵌入向量轉換為 NumPy 陣列來獲取其形狀
    embeddings_array = np.array(embeddings_list)
    print(embeddings_array.shape)
    
    # 返回嵌入向量的 list 形式
    return embeddings_list

# def to_openai_embeddings(text_list: list):
#     embeddings_list = []
#     for text in text_list:
#         response = openai.Engine("text-embedding-3-small").embeddings(
#             input=[text]
#         )
#         embeddings = response['data'][0]['embedding']
#         embeddings_list.append(embeddings)
    
#     # 將嵌入向量轉換為 NumPy 陣列來獲取其形狀
#     embeddings_array = np.array(embeddings_list)
#     print(embeddings_array.shape)
    
#     # 返回嵌入向量的 list 形式
#     return embeddings_list

# def to_openai_embeddings(text_list: list):
#     # 使用 OpenAI 的 "text-embedding-3-small" 模型來獲取文本嵌入向量
#     embeddings_response = openai.Embedding.create(
#         input=text_list,
#         model="text-embedding-3-small"
#     )
    
#     # 從回應中提取嵌入向量並轉換為 list of lists 形式
#     embeddings_list = [embedding['embedding'] for embedding in embeddings_response['data']]
    
#     # 將嵌入向量轉換為 NumPy 陣列來獲取其形狀
#     embeddings_array = np.array(embeddings_list)
#     print(embeddings_array.shape)
    
#     # 返回嵌入向量的 list 形式
#     return embeddings_list
