import 'package:collection/collection.dart';
import 'package:cs4225/data/client.dart';
import 'package:cs4225/model/country_sentiment.dart';
import 'package:equatable/equatable.dart';
import 'package:flutter_bloc/flutter_bloc.dart';

import '../data/data_point.dart';
import '../model/sentiment.dart';

class DataBloc extends Bloc<DataBlocEvent, DataBlocState> {
  final _client = Client();

  DataBloc() : super(const FetchDataInitial()) {
    on<DataBlocEvent>((DataBlocEvent event, Emitter emit) async {
      List<CountrySentiment> countrySentiments = [];
      Set<DateTime> shownDates = {};
      emit(const FetchDataLoading());

      try {
        final futures = [
          _client.getDataOf(event.chart.api),
          Future.delayed(const Duration(milliseconds: 1500))
        ];
        final response = await Future.wait(futures);

        groupBy(response.first.documentResponse as List<DataPoint>,
            (DataPoint dataPoint) => dataPoint.location).forEach((String key,
                List<DataPoint> values) =>
            countrySentiments.add(CountrySentiment.create(
                country: key,
                sentiments: values
                    .where((dataPoint) => dataPoint.date.isBefore(event.date))
                    .map((dataPoint) {
                  shownDates.add(dataPoint.date);
                  return Sentiment.create(dataPoint: dataPoint);
                }).toList())));
        shownDates.remove(shownDates.first);
        emit(FetchDataCompleted(
            countrySentiment: countrySentiments, shownDates: shownDates));
      } on ClientException catch (e) {
        emit(FetchDataError(toastMessage: e.errorMessage));
      }
    });
  }
}

class DataBlocEvent extends Equatable {
  final DateTime date;
  final Chart chart;

  const DataBlocEvent({required this.date, required this.chart});

  @override
  List<Object> get props => [date, chart];
}

abstract class DataBlocState extends Equatable {
  const DataBlocState();
}

class FetchDataInitial extends DataBlocState {
  const FetchDataInitial();
  @override
  List<Object?> get props => [];
}

class FetchDataLoading extends DataBlocState {
  const FetchDataLoading();
  @override
  List<Object?> get props => [];
}

class FetchDataCompleted extends DataBlocState {
  final List<CountrySentiment> countrySentiment;
  final Set<DateTime> shownDates;

  const FetchDataCompleted(
      {required this.countrySentiment, required this.shownDates});

  @override
  List<Object> get props => [countrySentiment, shownDates];
}

class FetchDataError extends DataBlocState {
  final String toastMessage;

  const FetchDataError({required this.toastMessage});

  @override
  List<Object> get props => [toastMessage];
}

enum Chart { overall, russia, ukraine }

extension ChartApi on Chart {
  String get api {
    switch (this) {
      case Chart.russia:
        return 'https://sentanaly.azurewebsites.net/api/sentimentsrussia';
      case Chart.overall:
        return 'https://sentanaly.azurewebsites.net/api/sentimentsoverall';
      case Chart.ukraine:
        return 'https://sentanaly.azurewebsites.net/api/sentimentsukraine';
    }
  }

  String get display {
    switch (this) {
      case Chart.russia:
        return 'Sentiment against Russia per country over time';
      case Chart.overall:
        return 'Overall sentiment per country over time';
      case Chart.ukraine:
        return 'Sentiment against Ukraine per country over time';
    }
  }

  String get name {
    switch (this) {
      case Chart.russia:
        return 'Russia';
      case Chart.overall:
        return 'Overall';
      case Chart.ukraine:
        return 'Ukraine';
    }
  }
}
