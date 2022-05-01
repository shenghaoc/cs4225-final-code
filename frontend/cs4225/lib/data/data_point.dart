import 'package:json_annotation/json_annotation.dart';

part 'data_point.g.dart';

@JsonSerializable()
class DataPoint {
  @JsonKey(required: true)
  final String location;

  @JsonKey(required: true)
  final DateTime date;

  @JsonKey(required: false)
  final double? sentiment;

  @JsonKey(required: true)
  final int count;

  const DataPoint(
      {required this.location,
      required this.date,
      required this.sentiment,
      required this.count});

  factory DataPoint.fromJson(Map<String, dynamic> json) =>
      _$DataPointFromJson(json);

  Map<String, dynamic> toJson() => _$DataPointToJson(this);
}
