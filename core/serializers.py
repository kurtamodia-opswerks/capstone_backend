from rest_framework import serializers

class DatasetSerializer(serializers.Serializer):
    upload_id = serializers.CharField()
    row_id = serializers.IntegerField()
    date = serializers.DateTimeField(allow_null=True, required=False)
    product_id = serializers.CharField(required=False, allow_null=True)
    product_name = serializers.CharField(required=False, allow_null=True)
    category = serializers.CharField(required=False, allow_null=True)
    quantity = serializers.IntegerField(required=False, allow_null=True)
    unit_price = serializers.FloatField(required=False, allow_null=True)
    sales = serializers.FloatField(required=False, allow_null=True)
