import 'package:cs4225/model/sentiment.dart';

class CountrySentiment {
  final String country;
  final List<Sentiment> sentiments;

  const CountrySentiment({required this.country, required this.sentiments});

  factory CountrySentiment.create(
      {required String country, required List<Sentiment> sentiments}) {
    sentiments.sort((sentiment1, sentiment2) =>
        sentiment1.rawDate.compareTo(sentiment2.rawDate));
    return CountrySentiment(country: country, sentiments: sentiments);
  }
}
