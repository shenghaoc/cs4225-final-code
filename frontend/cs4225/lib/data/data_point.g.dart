// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'data_point.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

DataPoint _$DataPointFromJson(Map<String, dynamic> json) {
  $checkKeys(
    json,
    requiredKeys: const ['location', 'date', 'count'],
  );
  return DataPoint(
    location: json['location'] as String,
    date: DateTime.parse(json['date'] as String),
    sentiment: (json['sentiment'] as num?)?.toDouble(),
    count: json['count'] as int,
  );
}

Map<String, dynamic> _$DataPointToJson(DataPoint instance) => <String, dynamic>{
      'location': instance.location,
      'date': instance.date.toIso8601String(),
      'sentiment': instance.sentiment,
      'count': instance.count,
    };
