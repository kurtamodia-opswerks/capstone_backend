from rest_framework import serializers

class DatasetSerializer(serializers.Serializer):
    upload_id = serializers.CharField()
    row_id = serializers.IntegerField()
    model = serializers.CharField(required=False, allow_null=True)
    year = serializers.IntegerField(required=False, allow_null=True)
    region = serializers.CharField(required=False, allow_null=True)
    color = serializers.CharField(required=False, allow_null=True)
    transmission = serializers.CharField(required=False, allow_null=True)
    mileage_km = serializers.FloatField(required=False, allow_null=True)
    price_usd = serializers.FloatField(required=False, allow_null=True)
    sales_volume = serializers.IntegerField(required=False, allow_null=True)