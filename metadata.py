import pyspark.sql.types as spark_types

files = {
 'users':'data/users/users.json',
 'messages':'data/messages/messages.json'
 }   
col_dtypes_messages= {
    "createdAt": spark_types.StringType(),
    "message": spark_types.StringType(),
    "receiverId": spark_types.IntegerType(),
    "id": spark_types.IntegerType(),
    "senderId": spark_types.IntegerType()
  }
struct_cols = ['profile','subscription']
col_dtypes_users= {
    "createdAt": spark_types.StringType(),
    "updatedAt": spark_types.StringType(),
    "firstName": spark_types.StringType(),
    "lastName": spark_types.StringType(),
    "address": spark_types.StringType(),
    "city": spark_types.StringType(),
    "country": spark_types.StringType(),
    "zipCode": spark_types.StringType(),
    "email": spark_types.StringType(),
    "birthDate": spark_types.StringType(),
    "profile.gender": spark_types.StringType(),
    "profile.gender": spark_types.StringType(),
    "profile.isSmoking": spark_types.BooleanType(),
    "profile.profession": spark_types.StringType(),
    "profile.income": spark_types.FloatType(),
    "subscription.createdAt": spark_types.StringType(),
    "subscription.startDate": spark_types.StringType(),
    "subscription.endDate": spark_types.StringType(),
    "subscription.status": spark_types.StringType(),
    "subscription.amount": spark_types.StringType(),
    "id": spark_types.IntegerType()
  }
