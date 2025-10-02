import pandas as pd
import uuid
from datetime import datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from db_connection import db
from core.serializers import DatasetSerializer
from core.models import dataset_collection

class DatasetUploadView(APIView):
    def post(self, request):
        file = request.FILES.get("file")
        if not file:
            return Response({"error": "No file uploaded"}, status=status.HTTP_400_BAD_REQUEST)

        try:

            EXPECTED_COLUMNS = ["model", "year", "region", "color", "transmission", "mileage_km", "price_usd", "sales_volume"]

            df = pd.read_csv(file)
            col_map = {c.lower(): c for c in EXPECTED_COLUMNS}
            df.columns = [col.strip().lower() for col in df.columns]
            df = df[[col for col in df.columns if col in col_map]]
            df = df.rename(columns=col_map)
            for col in EXPECTED_COLUMNS:
                if col not in df.columns:
                    df[col] = None
            df = df[EXPECTED_COLUMNS]
            df = df.where(pd.notnull(df), None)

            upload_id = f"upload_{uuid.uuid4().hex}"
            records = df.to_dict(orient="records")
            for idx, record in enumerate(records, start=1):
                record["upload_id"] = upload_id
                record["row_id"] = idx

            # Validate each record with serializer
            valid_records = []
            for rec in records:
                serializer = DatasetSerializer(data=rec)
                if serializer.is_valid():
                    valid_records.append(serializer.validated_data)
                else:
                    # skip or return error if invalid
                    return Response({
                        "error": "Validation failed",
                        "details": serializer.errors,
                        "row": rec
                    }, status=status.HTTP_400_BAD_REQUEST)

            if valid_records:
                dataset_collection.insert_many(valid_records)

            return Response({
                "message": "CSV uploaded successfully",
                "upload_id": upload_id,
                "rows_inserted": len(valid_records)
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        

    def get(self, request):
        upload_id = request.query_params.get("upload_id", None)

        query = {}
        if upload_id:
            query["upload_id"] = upload_id

        # Fetch from Mongo
        records = list(dataset_collection.find(query, {"_id": 0}))

        if not records:
            return Response({"message": "No records found"}, status=status.HTTP_404_NOT_FOUND)

        # Serialize
        serializer = DatasetSerializer(records, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

