import 'package:cs4225/data/data_point.dart';
import 'package:intl/intl.dart';

class Sentiment {
  final String country;
  final String date;
  final DateTime rawDate;
  final double sentiment;

  const Sentiment(
      {required this.country,
      required this.date,
      required this.rawDate,
      required this.sentiment});

  factory Sentiment.create({required DataPoint dataPoint}) => Sentiment(
      country: dataPoint.location,
      date: DateFormat('dd MMM').format(dataPoint.date.toLocal()),
      rawDate: dataPoint.date,
      sentiment: dataPoint.sentiment ?? 0.0);
}
