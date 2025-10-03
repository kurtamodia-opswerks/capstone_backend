import pandas as pd
import uuid
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from core.serializers import DatasetSerializer
from core.models import dataset_collection


class DatasetViewSet(viewsets.ViewSet):
    """
    A ViewSet for uploading datasets, retrieving them, 
    and fetching valid headers.
    """

    def create(self, request):
        """Handles CSV upload"""
        file = request.FILES.get("file")
        if not file:
            return Response({"error": "No file uploaded"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            EXPECTED_COLUMNS = [
                "model", "year", "region", "color", 
                "transmission", "mileage_km", "price_usd", "sales_volume"
            ]

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

            # Validate
            valid_records = []
            for rec in records:
                serializer = DatasetSerializer(data=rec)
                if serializer.is_valid():
                    valid_records.append(serializer.validated_data)
                else:
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

    def list(self, request):
        """Fetch records (optionally by upload_id)"""
        upload_id = request.query_params.get("upload_id")
        query = {"upload_id": upload_id} if upload_id else {}

        records = list(dataset_collection.find(query, {"_id": 0}))
        if not records:
            return Response({"message": "No records found"}, status=status.HTTP_404_NOT_FOUND)

        serializer = DatasetSerializer(records, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["get"], url_path="headers")
    def headers(self, request):
        """Return headers that have at least one valid value"""
        upload_id = request.query_params.get("upload_id")
        query = {"upload_id": upload_id} if upload_id else {}

        records = list(dataset_collection.find(query, {"_id": 0}))
        if not records:
            return Response({"message": "No records found"}, status=status.HTTP_404_NOT_FOUND)

        df = pd.DataFrame(records)
        valid_headers = [col for col in df.columns if df[col].notnull().any()]

        return Response({"valid_headers": valid_headers}, status=status.HTTP_200_OK)
    
    @action(detail=False, methods=["post"], url_path="aggregate")
    def aggregate(self, request):
        """
        Aggregate dataset directly in MongoDB.
        Supports x_axis, y_axis, aggregation function,
        and optional year range filtering.
        """
        upload_id = request.data.get("upload_id")
        x_axis = request.data.get("x_axis")
        y_axis = request.data.get("y_axis")
        agg_func = request.data.get("agg_func", "sum")
        year_from = request.data.get("year_from")
        year_to = request.data.get("year_to")

        if not upload_id or not x_axis or not y_axis:
            return Response(
                {"error": "upload_id, x_axis, and y_axis are required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Map agg functions to MongoDB operators
        valid_funcs = {
            "sum": "$sum",
            "avg": "$avg",
            "count": "$sum",  # handled differently below
            "min": "$min",
            "max": "$max",
        }
        if agg_func not in valid_funcs:
            return Response(
                {"error": f"Invalid agg_func. Choose from {list(valid_funcs.keys())}"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Build match filter
        match_stage = {"upload_id": upload_id}
        if year_from or year_to:
            year_filter = {}
            if year_from:
                year_filter["$gte"] = int(year_from)
            if year_to:
                year_filter["$lte"] = int(year_to)
            match_stage["year"] = year_filter

        # Build aggregation pipeline
        pipeline = [
            {"$match": match_stage},
            {"$group": {
                "_id": f"${x_axis}",
                y_axis: (
                    {"$sum": 1} if agg_func == "count"
                    else {valid_funcs[agg_func]: f"${y_axis}"}
                )
            }},
            {"$project": {
                x_axis: "$_id",
                y_axis: f"${y_axis}",
                "_id": 0
            }},
            {"$sort": {x_axis: 1}}  # 🔹 Sort ascending by x_axis
        ]

        try:
            result = list(dataset_collection.aggregate(pipeline))
            if not result:
                return Response({"message": "No records found"}, status=status.HTTP_404_NOT_FOUND)
            return Response(result, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)



