from datetime import datetime
from decimal import Decimal
from uuid import UUID
from typing import List
from store.core.exceptions import NotFoundException, InsertException
from store.schemas.product import ProductIn, ProductOut, ProductUpdate, ProductUpdateOut
from store.models.product import ProductModel
from store.db.mongo import db_client


class ProductUsecase:
    async def create(self, body: ProductIn) -> ProductOut:
        product_model = ProductModel(**body.model_dump())
        try:
            await db_client.get()["products"].insert_one(product_model.model_dump())
        except Exception:
            raise InsertException("Error creating product")
        
        return ProductOut(**product_model.model_dump())

    async def get(self, id: UUID) -> ProductOut:
        result = await db_client.get()["products"].find_one({"id": str(id)})
        
        if not result:
            raise NotFoundException("Product not found")
        
        return ProductOut(**result)

    async def query(self) -> List[ProductOut]:
        return [ProductOut(**item) async for item in db_client.get()["products"].find()]

    async def update(self, id: UUID, body: ProductUpdate) -> ProductUpdateOut:
        update_data = body.model_dump(exclude_unset=True)
        update_data["updated_at"] = datetime.utcnow()
        
        result = await db_client.get()["products"].find_one_and_update(
            {"id": str(id)},
            {"$set": update_data},
            return_document=True
        )
        
        if not result:
            raise NotFoundException("Product not found")
        
        return ProductUpdateOut(**result)

    async def delete(self, id: UUID) -> None:
        result = await db_client.get()["products"].find_one_and_delete({"id": str(id)})
        
        if not result:
            raise NotFoundException("Product not found")

    async def query_by_price_range(self, min_price: float, max_price: float) -> List[ProductOut]:
        query = {
            "price": {
                "$gt": Decimal128(str(min_price)),
                "$lt": Decimal128(str(max_price))
            }
        }
        
        return [ProductOut(**item) async for item in db_client.get()["products"].find(query)]