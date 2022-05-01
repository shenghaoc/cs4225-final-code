import 'dart:math';

import 'package:another_flushbar/flushbar.dart';
import 'package:cs4225/logic/data_bloc.dart';
import 'package:cs4225/model/sentiment.dart';
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_spinkit/flutter_spinkit.dart';
import 'package:intl/intl.dart';
import 'package:syncfusion_flutter_charts/charts.dart';

void main() {
  return runApp(_ChartApp());
}

class _ChartApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      home: BlocProvider<DataBloc>(
          create: (context) => DataBloc(), child: const _MyHomePage()),
    );
  }
}

class _MyHomePage extends StatefulWidget {
  const _MyHomePage({Key? key}) : super(key: key);

  @override
  _MyHomePageState createState() => _MyHomePageState();
}

class _MyHomePageState extends State<_MyHomePage> {
  late DataBloc _bloc;
  late Chart _chart;
  late DateTime _dateTime;
  late DateTime? _selectedDateTime;

  @override
  void initState() {
    super.initState();
    _chart = Chart.overall;
    _dateTime = DateTime.now();
    _selectedDateTime = null;
    _bloc = BlocProvider.of<DataBloc>(context);
    _bloc.add(DataBlocEvent(date: _dateTime, chart: _chart));
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(body: BlocBuilder<DataBloc, DataBlocState>(
      builder: (context, state) {
        if (state is FetchDataError) {
          Flushbar(
            animationDuration: const Duration(milliseconds: 300),
            borderRadius: BorderRadius.circular(10),
            padding: const EdgeInsets.all(24),
            margin: const EdgeInsets.only(top: 12, left: 12, right: 12),
            backgroundColor: Colors.redAccent,
            flushbarStyle: FlushbarStyle.FLOATING,
            flushbarPosition: FlushbarPosition.TOP,
            messageText: Text(state.toastMessage,
                textAlign: TextAlign.center,
                style: const TextStyle(
                    color: Colors.white,
                    fontStyle: FontStyle.italic,
                    fontSize: 14)),
            duration: const Duration(seconds: 2),
          );
        }

        if (state is FetchDataCompleted) {
          return Container(
            padding: const EdgeInsets.all(32),
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                Text(
                    '${_chart.display} '
                    '(${DateFormat('dd MMM').format(state.shownDates.first.toLocal())} to '
                    '${DateFormat('dd MMM').format(state.shownDates.last.toLocal())})',
                    style: const TextStyle(
                      fontFamily: 'Lato',
                      fontSize: 24,
                      height: 1.25,
                      fontWeight: FontWeight.w700,
                    )),
                const SizedBox(height: 24),
                Expanded(
                  child: SfCartesianChart(
                      isTransposed: false,
                      primaryXAxis: CategoryAxis(),
                      legend: Legend(isVisible: true),
                      tooltipBehavior: TooltipBehavior(enable: true),
                      series: <ChartSeries<Sentiment, String>>[
                        ...state.countrySentiment.map((countrySentiment) =>
                            _line(
                                sentiments: countrySentiment.sentiments,
                                country: countrySentiment.country))
                      ]),
                ),
                const SizedBox(height: 60),
                Row(
                  mainAxisAlignment: MainAxisAlignment.start,
                  children: [
                    if (state.shownDates.length > 1)
                      DropdownButton(
                        value: _selectedDateTime,
                        hint: Padding(
                          padding: const EdgeInsets.all(8),
                          child: Row(
                            children: const [
                              Icon(Icons.search, size: 12),
                              SizedBox(width: 8),
                              Text('Set latest date',
                                  style: TextStyle(
                                    fontFamily: 'Lato',
                                    fontSize: 12,
                                    fontWeight: FontWeight.w700,
                                  ))
                            ],
                          ),
                        ),
                        items: state.shownDates
                            .map((date) => DropdownMenuItem(
                                child: Text(DateFormat('dd MMM')
                                    .format(date.toLocal())),
                                value: date))
                            .toList(),
                        onChanged: (DateTime? value) {
                          _selectedDateTime = value;
                          _dateTime = value?.add(const Duration(minutes: 1)) ??
                              DateTime.now();
                          return _bloc.add(
                              DataBlocEvent(chart: _chart, date: _dateTime));
                        },
                      ),
                    const SizedBox(width: 40),
                    DropdownButton(
                      value: _chart,
                      hint: Padding(
                        padding: const EdgeInsets.all(8),
                        child: Row(
                          children: [
                            const Icon(Icons.add_chart, size: 12),
                            const SizedBox(width: 8),
                            Text(_chart.name,
                                style: const TextStyle(
                                  fontFamily: 'Lato',
                                  fontSize: 12,
                                  fontWeight: FontWeight.w700,
                                ))
                          ],
                        ),
                      ),
                      items: Chart.values
                          .map((chart) => DropdownMenuItem(
                              child: Text(chart.name), value: chart))
                          .toList(),
                      onChanged: (Chart? value) {
                        _chart = value ?? Chart.overall;
                        return _bloc
                            .add(DataBlocEvent(chart: _chart, date: _dateTime));
                      },
                    ),
                    const SizedBox(width: 40),
                    ElevatedButton.icon(
                      onPressed: () async {
                        _selectedDateTime = null;
                        _dateTime = DateTime.now();
                        _chart = Chart.overall;
                        return _bloc
                            .add(DataBlocEvent(chart: _chart, date: _dateTime));
                      },
                      label: const Text('Reset'),
                      icon: const Icon(Icons.refresh_sharp),
                      style: ButtonStyle(
                        backgroundColor:
                            MaterialStateProperty.all<Color>(Colors.white54),
                        foregroundColor:
                            MaterialStateProperty.all<Color>(Colors.black),
                      ),
                    )
                  ],
                )
              ],
            ),
          );
        }

        return const Center(child: SpinKitWave(color: Colors.greenAccent));
      },
    ));
  }

  LineSeries<Sentiment, String> _line(
          {required List<Sentiment> sentiments, required String country}) =>
      LineSeries<Sentiment, String>(
          color: Colors.primaries[
              Random(country.hashCode).nextInt(Colors.primaries.length)],
          dataSource: sentiments,
          xValueMapper: (Sentiment sentiment, _) => sentiment.date,
          yValueMapper: (Sentiment sentiment, _) => sentiment.sentiment,
          name: country);
}
