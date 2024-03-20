import os
from pathlib import Path
import requests
import pandas as pd
from typing import Union
from .data_utils import read_data
import numpy as np
from collections import defaultdict
from time import sleep
from qdrant_client import QdrantClient
from qdrant_client.http import models, exceptions
from qdrant_client.models import Distance, VectorParams, PointStruct
from .log_utils import logger


class QdrantDBWrapper:
    def __init__(self) -> None:
        self.ROOT_DIR = Path(__file__).absolute().parents[1]
        self.config = read_data(str(self.ROOT_DIR / "config.yaml"))["DATABASE"]
        self.HOST = self.config["QDRANT"]["HOST"]
        self.PORT = self.config["QDRANT"]["PORT"]
        self.API_KEY = self.config["QDRANT"]["API_KEY"]
        QDRANT_URI = os.environ.get("QDRANT_URI", f"{self.HOST}:{self.PORT}")
        self.CONNECT_URI = f"http://{QDRANT_URI}"
        self.client = QdrantClient(url=self.CONNECT_URI,  api_key=self.API_KEY)
        print(f"CONNECT_URI: {self.CONNECT_URI}")
        logger.info(f"CONNECT_URI: {self.CONNECT_URI}")

    def use_collection(self, collection_name: str, **kwargs):
        try:
            self.collection = self.client.get_collection(collection_name)
        except Exception as err:
            logger.error(err)
            kwargs = defaultdict(lambda: None, kwargs)
            self.collection = self.recreate_collection(
                collection_name=collection_name, size=kwargs["size"] or 100, 
                distance=kwargs["distance"] or Distance.COSINE,
            )
            logger.info(f"Created collection: {collection_name}")
        return self.collection

    def recreate_collection(self, collection_name: str, **kwargs) -> None:
        kwargs = defaultdict(lambda: None, kwargs)
        vectors_config = VectorParams(
            size=kwargs["size"] or 100, distance=kwargs["distance"] or Distance.COSINE,
        )
        self.client.recreate_collection(
            collection_name=collection_name, vectors_config=vectors_config,
        )
        return self.use_collection(collection_name=collection_name)

    def upsert(self, collection_name: str, data: pd.DataFrame, embeddings: list):
        assert len(data) == len(embeddings)
        assert "idx" in data, Exception("need idx column in data with unique id value.")
        
        self.collection = self.use_collection(collection_name=collection_name, size=len(embeddings[0]))
        row_num = self.count(collection_name=collection_name, count_filter=None).count
        all_data = self.scroll(collection_name=collection_name, limit=row_num, scroll_filter=None) if row_num else None
        logger.info(f"Total data in collection: {row_num}")
        
        if all_data:
            logger.info(all_data)
            try:
                all_urls = [item.id for item in all_data[0]]
            except KeyError as err:
                logger.error(err)
                self.detect_broken_data(collection_name=collection_name)
                all_urls = [item.id for item in all_data[0]]
        else:
            all_urls = []

        data["temp_vector"] = embeddings
        _data = data[~data["idx"].isin(all_urls)].reset_index(drop=True)
        logger.info(_data)
        _embeddings = _data["temp_vector"].tolist()
        print(f"Valid data to upsert: {len(_data)}")
        if not _data.empty and len(_data) == len(_embeddings):
            self.client.upsert(
                collection_name=collection_name,
                # points=[PointStruct(**vector) for vector in vectors],
                points=models.Batch(
                    ids=_data["idx"].tolist(),
                    vectors=_embeddings,
                    payloads=_data.drop(["idx", "temp_vector"], axis=1).to_dict("records"),
                ),
            )

    def search(
        self,
        collection_name: str,
        query_vector: np.ndarray,
        query_filter: Union[models.Filter, None],
        limit: int,
    ):
        return self.client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=limit,
        )

    def scroll(
        self,
        collection_name: str,
        limit: Union[int, None],
        scroll_filter: Union[models.Filter, None],
        with_payload: bool = True,
        with_vectors: bool = False,
    ):
        return self.client.scroll(
            collection_name=collection_name,
            limit=limit,
            with_payload=with_payload,
            with_vectors=with_vectors,
            scroll_filter=scroll_filter,
        )

    def count(
        self,
        collection_name: str,
        count_filter: Union[models.Filter, None],
        exact: bool = True,
    ):
        return self.client.count(
            collection_name=collection_name, count_filter=count_filter, exact=exact
        )

    def retrieve(self, collection_name: str, ids: list):
        return self.client.retrieve(
            collection_name=collection_name,
            ids=ids,
            with_payload=True,
            with_vectors=True,
        )
    
    def delete(self, collection_name: str, ids: list):
        self.client.delete(
            collection_name=collection_name,
            points_selector=models.PointIdsList(
                points=ids,
            )
        )
        return f'Deleted ids: {ids}'

    def update_payload(self, collection_name: str, id: int, key: str, value: str):
        item = self.retrieve(collection_name=collection_name, ids=[id])[0]
        item_payload = item.payload
        item_payload.update({key: value})
        self.client.overwrite_payload(
            collection_name=collection_name,
            payload=item_payload,
            points=[id],
        )
        sleep(1)
        check_item = self.retrieve(collection_name=collection_name, ids=[id])[0]
        return check_item

    def to_dataframe(self, qdrant_records):
        rows = []
        for item in qdrant_records:
            item_payload = item.payload
            item_payload.update({"id": item.id, "vector": item.vector})
            rows.append(item_payload)

        return pd.DataFrame(rows)

    def get_metrics(self):
        metric_uri = f"{self.CONNECT_URI}/metrics"
        metrics = requests.get(metric_uri).content.decode("utf-8")
        metrics = [item for item in metrics.split("\n") if item]
        metrics = {
            item.split(" ")[0]: item.split(" ")[1]
            for item in metrics
            if len(item.split(" ")) == 2
        }
        return metrics

    def detect_broken_data(self, collection_name: str):
        count = self.count(collection_name=collection_name, count_filter=None).count
        all_data = self.scroll(collection_name=collection_name, limit=count, scroll_filter=None)[0]
        broken_ids = []
        for item in all_data:
            if "idx" not in item.payload:
                broken_ids.append(item.id)
        self.delete(collection_name=collection_name, ids=broken_ids)

    def remove_repeat_data(self, collection_name: str, subset: list):
        count = self.count(collection_name=collection_name, count_filter=None).count
        all_data = self.scroll(collection_name=collection_name, limit=count, scroll_filter=None)[0]
        all_data = self.to_dataframe(qdrant_records=all_data)
        all_ids = all_data["id"].tolist()
        all_data = all_data.drop_duplicates(subset=subset).reset_index(drop=True)
        keep_ids = all_data["id"].tolist()
        id_to_remove = [i for i in all_ids if i not in keep_ids]
        self.delete(collection_name=collection_name, ids=id_to_remove)
