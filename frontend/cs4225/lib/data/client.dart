import 'dart:convert';

import 'package:cs4225/data/document_response.dart';
import 'package:dio/dio.dart';
import 'package:logger/logger.dart';

class Client {
  final logger = Logger();
  final dio = Dio();

  Future<DocumentResponse> getDataOf(String api) async {
    try {
      final response = await dio.get(api);
      final data = jsonDecode(response.data);
      logger.d(data['documentResponse']);
      // logger.d(data);
      return DocumentResponse.fromJson(data);
    } on DioError catch (dioError, stackTrace) {
      logger.e(dioError.message, dioError, stackTrace);
      throw ClientException(dioError.message);
    }
  }
}

class ClientException implements Exception {
  final String errorMessage;

  const ClientException(this.errorMessage);

  String getToastMessage() {
    return 'Error occurred while fetching data from server';
  }
}
