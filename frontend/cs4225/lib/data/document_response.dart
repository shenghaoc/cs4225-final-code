import 'package:cs4225/data/data_point.dart';
import 'package:json_annotation/json_annotation.dart';

part 'document_response.g.dart';

@JsonSerializable()
class DocumentResponse {
  @JsonKey(required: true)
  final List<DataPoint> documentResponse;

  const DocumentResponse({required this.documentResponse});

  factory DocumentResponse.fromJson(Map<String, dynamic> json) =>
      _$DocumentResponseFromJson(json);

  Map<String, dynamic> toJson() => _$DocumentResponseToJson(this);
}
