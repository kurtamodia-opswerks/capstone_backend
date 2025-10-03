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
        Aggregate dataset based on X and Y axes and return grouped data.
        User can choose aggregation function (sum, mean, count, min, max, etc.)
        and optionally filter by year range (year_from, year_to).
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

        # Fetch dataset
        records = list(dataset_collection.find({"upload_id": upload_id}, {"_id": 0}))
        if not records:
            return Response({"message": "No records found"}, status=status.HTTP_404_NOT_FOUND)

        df = pd.DataFrame(records)

        if x_axis not in df.columns or y_axis not in df.columns:
            return Response(
                {"error": "Invalid x_axis or y_axis"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Ensure numeric for y_axis
        df[y_axis] = pd.to_numeric(df[y_axis], errors="coerce")

        # ✅ Apply year range filter if present
        if "year" in df.columns:
            if year_from:
                df = df[df["year"] >= int(year_from)]
            if year_to:
                df = df[df["year"] <= int(year_to)]

        # Validate agg_func
        valid_funcs = {
            "sum": "sum",
            "avg": "mean",
            "mean": "mean",
            "count": "count",
            "min": "min",
            "max": "max"
        }
        if agg_func not in valid_funcs:
            return Response(
                {"error": f"Invalid agg_func. Choose from {list(valid_funcs.keys())}"},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            grouped = df.groupby(x_axis)[y_axis].agg(valid_funcs[agg_func]).reset_index()
            result = grouped.to_dict(orient="records")
            return Response(result, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

