#r "Microsoft.WindowsAzure.Storage"
#r "DynamicJson.dll"
#r "Newtonsoft.Json"

using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.IO;
using System.Collections;
using static System.Environment;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Codeplex.Data;
using Newtonsoft.Json;

using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json.Linq;


public static async Task Run(string myIoTHubMessage,　CloudTable inputBusDelayTable,　CloudTable inputBusLocationTable,　CloudTable inputBusStopTable,　CloudTable inputLastRouteTable, CloudTable inputAlertManagementTable, ICollector <BusLocationTableLPWA> outputBusLocationTable,　ICollector <BusDelayTableLPWA> outputBusDelayTable, ICollector <BatteryInformationTable> outputBatteryInformationTable, ICollector <AlertManagementTable> outputAlertManagementTable, ILogger log)
{
    DateTime dt = DateTime.Now;
    log.LogInformation($"{dt}");
    //IoTHubから受信するメッセージ
    string receivedMessage = myIoTHubMessage;
    string receivedMessageBinary = null;
    // パケット識別情報
    int informationType = 0;
    int contentType = 0;
    //路線KEY（4桁）
    string routeKey = null;
    int routeKeyInt = 0;
    // 最終便を示すフラグ
    int flagLast = 0;
    int flagStatus = 0;
    //便番号
    int serviceNum = 0;
    //時刻（HH）
    int hour = 0;
    //時刻（MM）
    int min = 0;
    //時刻（HH）
    string hourStr = null;
    //時刻（MM）
    string minStr = null;
    //次バス停のシーケンス番号
    int stopSequence = 0;
    //相対緯度
    double busLat = 0.0;
    //相対経度
    double busLon = 0.0;
    //絶対緯度
    double absoluteBusLat = 0.0;
    //絶対経度
    double absoluteBusLon = 0.0;
    //基準点緯度
    double basicLat = 35.051167;
    //基準点経度
    double basicLon = 136.997024;
    //接近状態
    int approachStatus1 = 0;
    int approachStatus2 = 0;
    log.LogInformation($"receivedMessage:{receivedMessage}");
                          
    //GTFSにおけるroute_id，service_id，trip_idを格納する変数
    string routeId = null;
    string serviceId = null;
    string tripId = null;

    //バスの運行状態を表すフラグを格納する変数
    bool serviceFlag = false;

    // LastRouteTableに格納してあるシーケンス番号
    int sequenceNumLast = 0;
    // LastRouteTableに格納してあるルートID
    int routeIdLast = 0;
    // LastRouteTableに格納してあるデバイスID
    string deviceIdLast = null;
    // LastRouteTableに格納してあるバス停名
    string stationNameLast = null;

    // デバイスID
    string devEUI = null;

    // 1つ前の時刻（アラート管理用）
    string PreviousTime = null;

    //受信した16進表記の文字列を2進表記の文字列に変換
    for(int i = 0; i < receivedMessage.Length; i++){
        string tmpString = Convert.ToString(receivedMessage[i]);
        string tmpBinary = Convert.ToString(Convert.ToInt64(tmpString, 16), 2).PadLeft(4, '0');
        // log.LogInformation($"tmpBinary:{tmpBinary}");
        receivedMessageBinary = receivedMessageBinary + tmpBinary;
        // log.LogInformation($"receivedMessageBinary:{receivedMessageBinary}");
    }
    // パケット識別情報取得
    informationType = Convert.ToInt32(receivedMessageBinary.Substring(0,4), 2);
    log.LogInformation($"Get InformationType : {informationType}");
    contentType = Convert.ToInt32(receivedMessageBinary.Substring(4,4), 2);
    log.LogInformation($"Get ContentType : {contentType}");

    // バス停に現在時刻を送信
    if(informationType==7 && contentType==2){
        DateTime dt_idEnd = DateTime.Now;
        log.LogInformation($"Message Idintify Time{dt_idEnd - dt}");
        string resultSendUnixTime = null;
        devEUI = receivedMessage.Substring(2,16);
        log.LogInformation($"Get DevEUI : {devEUI}");
        sendUnixTime(devEUI, dt,out resultSendUnixTime,log);
        log.LogInformation($"Get ResultSendUnixTime : {resultSendUnixTime}");
        return;
    }

    //コンテンツ切り替え応答メッセージを受信したときの処理
    if(informationType == 6 && contentType == 2){
        string currentContentHex = receivedMessage.Substring(2,3);
        string changedTimeUnixHex = receivedMessage.Substring(5,8);
        devEUI = receivedMessage.Substring(14,16);

        string currentContent = Convert.ToInt32(currentContentHex, 16).ToString();
        string changedTimeUnix = Convert.ToInt32(changedTimeUnixHex, 16).ToString();

        log.LogInformation($"DevEUI : {devEUI} , CurrentContent : {currentContent} , ChangedTime : {changedTimeUnix}");
        return;
    }

    //バナー情報のACKを受信したとき
    if(informationType == 3 && contentType == 3){
        string seqNum = receivedMessage.Substring(2,1);

        string bannerAckJsonPath = "..\\..\\..\\..\\..\\home\\site\\wwwroot\\bannerInfo\\bannerACK.json";
        
        string bannerACKStr = File.ReadAllText(bannerAckJsonPath); 
        
        //JSON文字列をJson.NETのオブジェクトに変換
        JObject bannerACKJson = JObject.Parse(bannerACKStr); 
        
        bannerACKJson["bannerAck"][Int32.Parse(seqNum)]["ResponseStatus"] = 1; 

        File.WriteAllText(bannerAckJsonPath, bannerACKJson.ToString());

        return;
    }

    //バッテリ情報を受信したときの処理
    if(informationType == 2 && contentType == 1){
        string BusStopId = Convert.ToString(Convert.ToInt32(receivedMessageBinary.Substring(8,11),2)) + "-" + Convert.ToString(Convert.ToInt32(receivedMessageBinary.Substring(19,2),2));
        string Time = Convert.ToString(Convert.ToInt32(receivedMessageBinary.Substring(21,5),2)) + ":" + Convert.ToString(Convert.ToInt32(receivedMessageBinary.Substring(26,6),2));
        double Voltage = Convert.ToDouble(Convert.ToInt32(receivedMessageBinary.Substring(32,8),2))/10;
        int BatteryLevel = Convert.ToInt32(receivedMessageBinary.Substring(40,4),2);
        await Task.Run(() => insertBatteryInformationTableMethod(log, outputBatteryInformationTable, BusStopId, Time, Voltage, BatteryLevel));
        log.LogInformation($"Insert BatteryInformationTable : BusStopId = {BusStopId}, Time = {Time}, Voltage = {Voltage}, BatteryLevel = {BatteryLevel}");

        await Task.Run(() => insertAlertManagementTableMethod(log, outputAlertManagementTable, BusStopId, Time, BatteryLevel));

        return;
    }

/*
    //ここから旧車載器の仕様
    //車載器から送信される各種情報（路線KEY，便番号，最終便判定フラグ,時刻情報（HH，MM），次バス停のシーケンス番号，緯度，経度，接近状態）を取得
    //routeKey = receivedMessageBinary.Substring(8,10);
    routeKey = receivedMessageBinary.Substring(14,4);
    routeKeyInt = Convert.ToInt32(routeKey, 2);
    log.LogInformation($"Get RouteKey : {routeKey}");
    serviceNum = Convert.ToInt32(receivedMessageBinary.Substring(18,8), 2);
    log.LogInformation($"Get ServiceNum : {serviceNum}");
    flagLast = Convert.ToInt32(receivedMessageBinary.Substring(26,1), 2);
    log.LogInformation($"Get FlagLast : {flagLast}");
    busLat = Convert.ToInt64(receivedMessageBinary.Substring(27,17), 2);
    log.LogInformation($"Get Bus Latitude : {busLat}");
    busLon = Convert.ToInt64(receivedMessageBinary.Substring(44,17), 2);
    log.LogInformation($"Get Bus Longitude : {busLon}");
    hourStr = Convert.ToString(Convert.ToInt32(receivedMessageBinary.Substring(61,5), 2));
    log.LogInformation($"Get Hour Str : {hourStr}");
    minStr = Convert.ToString(Convert.ToInt32(receivedMessageBinary.Substring(66,6), 2));
    log.LogInformation($"Get Minute Str : {minStr}");
    stopSequence = Convert.ToInt32(receivedMessageBinary.Substring(72,7), 2);
    log.LogInformation($"Get Stop Sequence : {stopSequence}");
    approachStatus1 = Convert.ToInt32(receivedMessageBinary.Substring(79,3), 2);
    log.LogInformation($"Get Stop approachStatus1 : {approachStatus1}");
    approachStatus2 = Convert.ToInt32(receivedMessageBinary.Substring(82,3), 2);
    log.LogInformation($"Get Stop approachStatus2 : {approachStatus2}");

    // 最終便だった場合
    if(flagLast == 1){
        // LastRouteTableからデータを取得
        foreach (LastRouteTableEntity entity in await inputLastRouteTable.ExecuteQuerySegmentedAsync(new TableQuery<LastRouteTableEntity>(), null)){
            log.LogInformation($"Get FlagLast : {flagLast}");
            // シークエンス番号を取得
            sequenceNumLast = entity.SequenceNumber;
            log.LogInformation($"Get sequenceNumLast : {sequenceNumLast}");
            // ルートIDを取得
            routeIdLast = entity.RouteID;
            log.LogInformation($"Get routeIdLast : {routeIdLast}");
            // 最終便バス停確認
            if(sequenceNumLast == (stopSequence-1) && routeIdLast == routeKeyInt){
                // デバイスIDを取得
                deviceIdLast = entity.DeviceID;
                log.LogInformation($"Get deviceIdLast : {deviceIdLast}");
                // バス停名を取得(ログ出力のために取得)
                stationNameLast = entity.StationName;
                log.LogInformation($"Get stationNameLast : {stationNameLast}");
                break;
            }
        }
        // バス停に運行終了メッセージを送信
        string resultSendFinish = null;
        sendFinish(sequenceNumLast, routeIdLast, deviceIdLast, out resultSendFinish);
        log.LogInformation($"{stationNameLast}のバス停への送信結果:{resultSendFinish}");
    }
    //ここまで旧車載器の仕様
*/

    //ここから新車載器
    //車載器から送信される各種情報（路線KEY，便番号，最終便判定フラグ,時刻情報（HH，MM），次バス停のシーケンス番号，緯度，経度，接近状態）を取得
    //routeKey = receivedMessageBinary.Substring(8,10);
    routeKey = receivedMessageBinary.Substring(14,4);
    routeKeyInt = Convert.ToInt32(routeKey, 2);
    log.LogInformation($"Get RouteKey : {routeKey}");
    serviceNum = Convert.ToInt32(receivedMessageBinary.Substring(18,8), 2);
    log.LogInformation($"Get ServiceNum : {serviceNum}");
    flagStatus = Convert.ToInt32(receivedMessageBinary.Substring(26,3), 2);
    log.LogInformation($"Get FlagStatus : {flagStatus}");
    busLat = Convert.ToInt64(receivedMessageBinary.Substring(29,17), 2);
    log.LogInformation($"Get Bus Latitude : {busLat}");
    busLon = Convert.ToInt64(receivedMessageBinary.Substring(46,17), 2);
    log.LogInformation($"Get Bus Longitude : {busLon}");
    hourStr = Convert.ToString(Convert.ToInt32(receivedMessageBinary.Substring(63,5), 2));
    log.LogInformation($"Get Hour Str : {hourStr}");
    minStr = Convert.ToString(Convert.ToInt32(receivedMessageBinary.Substring(68,6), 2));
    log.LogInformation($"Get Minute Str : {minStr}");
    stopSequence = Convert.ToInt32(receivedMessageBinary.Substring(74,7), 2);
    log.LogInformation($"Get Stop Sequence : {stopSequence}");
    approachStatus1 = Convert.ToInt32(receivedMessageBinary.Substring(81,3), 2);
    log.LogInformation($"Get Stop approachStatus1 : {approachStatus1}");
    approachStatus2 = Convert.ToInt32(receivedMessageBinary.Substring(84,3), 2);
    log.LogInformation($"Get Stop approachStatus2 : {approachStatus2}");

    // 最終便だった場合
    if(flagStatus == 3){
        // LastRouteTableからデータを取得
        foreach (LastRouteTableEntity entity in await inputLastRouteTable.ExecuteQuerySegmentedAsync(new TableQuery<LastRouteTableEntity>(), null)){
            log.LogInformation($"Get flagStatus : {flagStatus}");
            // シークエンス番号を取得
            sequenceNumLast = entity.SequenceNumber;
            log.LogInformation($"Get sequenceNumLast : {sequenceNumLast}");
            // ルートIDを取得
            routeIdLast = entity.RouteID;
            log.LogInformation($"Get routeIdLast : {routeIdLast}");
            // 最終便バス停確認
            if(sequenceNumLast == (stopSequence-1) && routeIdLast == routeKeyInt){
                // デバイスIDを取得
                deviceIdLast = entity.DeviceID;
                log.LogInformation($"Get deviceIdLast : {deviceIdLast}");
                // バス停名を取得(ログ出力のために取得)
                stationNameLast = entity.StationName;
                log.LogInformation($"Get stationNameLast : {stationNameLast}");
                break;
            }
        }
        // バス停に運行終了メッセージを送信
        string resultSendFinish = null;
        sendFinish(sequenceNumLast, routeIdLast, deviceIdLast, out resultSendFinish);
        log.LogInformation($"{stationNameLast}のバス停への送信結果:{resultSendFinish}");
    }    
    //ここまで新車載器

    //位置情報の復元（基準点の座標（basicLat，basicLon）を加算）
    absoluteBusLat = busLat/1000000 + basicLat;
    log.LogInformation($"バスの絶対緯度 : {absoluteBusLat}");
    absoluteBusLon = busLon/1000000 + basicLon;
    log.LogInformation($"バスの絶対経度 : {absoluteBusLon}");

    //時刻情報をストレージへ格納する形（Datatime型）に変換
    string receivedtime = hourStr + ":" + minStr;
    DateTime time = DateTime.Parse(receivedtime);
    log.LogInformation($"受信時刻 : {time}");

    
    //GTFSのroute_idを取得 空白が返ってくる
    getRouteId(routeKeyInt.ToString("D4"), out routeId);
    log.LogInformation($"Get RouteId:{routeId}");
    //GTFSのservice_idを取得
    getServiceId(time, out serviceId);
    log.LogInformation($"Get ServiceId:{serviceId}");
    //GTFSのtrip_idを取得
    getTripId(routeId, serviceId, serviceNum, out tripId);
    log.LogInformation($"Get TripId:{tripId}");
    //バスの運行状態を取得
    getServiceFlag(tripId, time, out serviceFlag);
    log.LogInformation($"Get ServiceFlag:{serviceFlag}");
    //Azure上における30分前の時刻
    DateTime timeBefore = DateTime.UtcNow.AddMinutes(-30);
    log.LogInformation($"Get TimeBefore:{timeBefore}");
    //朝の最初の時間帯のみ前日のものを引き継ぐために前日の最終便辺りの時間帯前の時刻を取得
    if(((Int16.Parse(time.ToString("HHmm"))) >= 600) && ((Int16.Parse(time.ToString("HHmm"))) <= 700))
    {
        timeBefore = DateTime.UtcNow.AddHours(-10);
    }

    //BusLocationTableLPWAに位置情報などを格納
    await Task.Run(() => inputBusLocationTableMethod(log, outputBusLocationTable, inputBusLocationTable, routeKey, time, absoluteBusLat, absoluteBusLon, tripId, serviceNum, serviceFlag, approachStatus2, timeBefore));
   
    if(approachStatus2 == 3 || approachStatus1 == 3)
    {
        log.LogInformation("Started InputBusDelayTableMethod!");
        string busStopId = null;
        string[] busStopIds = null;
        getBusStopIds(tripId, out busStopIds);
        string line = "";
        int delayTime = 0;
        int delayTimeSeconds = 0;
        //GTFSデータのtrips.txtを読み込む
        using (StreamReader sr = new StreamReader("..\\..\\..\\..\\..\\home\\site\\wwwroot\\data\\GTFS-Data\\stop_times.txt"))
        {
            //1行ずつ読み込む
            while((line = sr.ReadLine()) != null)
            {
                string[] str = line.Split(',');
                

                if(tripId.Equals(str[0]) && stopSequence.ToString().Equals(str[4]))
                {
                    busStopId = str[3];
                    log.LogInformation($"Arrived at:{busStopId}");

                    TimeSpan span1 = TimeSpan.Parse(str[1]);
                    log.LogInformation($"Expected Arrival Time:{span1.ToString(@"hh\:mm\:ss")}");

                    TimeSpan span2 = TimeSpan.Parse(receivedtime);
                    log.LogInformation($"Arrival Time:{span2.ToString(@"hh\:mm\:ss")}");

                    TimeSpan delaySpan = span2- span1;
                    log.LogInformation($"Delay Time:{delaySpan.ToString(@"hh\:mm\:ss")}");

                    delayTime = (int)delaySpan.Minutes;
                    delayTimeSeconds = (int)delaySpan.Seconds;

                    if(delayTimeSeconds >= 30){
                        delayTime += 1;
                    }

                    log.LogInformation($"遅延時間:{delayTime}");
                    break;
                }
            }
            log.LogInformation("Start Delay Table Input");
            await Task.Run(() => inputBusDelayTableMethod(outputBusDelayTable, inputBusDelayTable, log, busStopId, routeKey, tripId, time, serviceNum, delayTime, stopSequence, approachStatus1, approachStatus2, timeBefore));
        }
    }
}

  /// <summary>
  ///  路線Keyから路線IDを得る
  /// <param name="busKey">路線情報と対応する4桁の文字列型の数字</param>
  /// <param name="routeId">GTFSデータに示されている路線情報(出力)</param>
  /// </summary>
public static void getRouteId(string busKey, out string routeId)
{ 
    //値の初期化
    string line = "";
    routeId = null;

    //busKeyとGTFSデータのroute_idの対応テキストファイルを読み込む
    using (StreamReader sr = new StreamReader("..\\..\\..\\..\\..\\home\\site\\wwwroot\\data\\CorrespondenceTable\\RouteCorrespondence.txt"))
    {
        //1行ずつ読み込む
        while ((line = sr.ReadLine()) != null)
        {
            //データはroute_key,route_idの並びとなっている
            //取得したデータをカンマ区切りで分ける
            string[] str = line.Split(',');
            //路線情報が一致するか判定
            if (busKey == str[0])
            {
                //GTFSにおけるroute_idを取得
                routeId = str[1];
                break;
            }
        }
    }

}
  /// <summary>
  ///  時刻情報からserviceIdを得る
  /// <param name="time">バスの位置情報を取得した際の時刻</param>
  /// <param name="serviceId">GTFSにおけるservice_id(出力)</param>
  /// </summary>
public static void getServiceId(DateTime time, out string serviceId)
{
    //値の初期化
    string line = "";
    string day = time.ToString("yyyyMMdd");
    serviceId = null;

    //GTFSデータのcalendar_dates.txtを読み込む
    using (StreamReader sr = new StreamReader("..\\..\\..\\..\\..\\home\\site\\wwwroot\\data\\GTFS-Data\\calendar_dates.txt"))
    {
        //一行ずつ最後まで読み込む
        while ((line = sr.ReadLine()) != null)
        {
            //取得したデータをカンマ区切りで分ける
            string[] str = line.Split(',');

            //データはservice_id,date,exception_typeの並びとなっている
            //指定した日付と運行区分が適応されるか判定
            if ((day.Equals(str[1])) && (str[2].Equals("1")))
            {
                serviceId = str[0];
                break;
            }
        }

    }

    //calendar_dates.txtで日付の指定がなかった場合のservice_idの対応確認
    if (serviceId == null)
    {
        //GTFSデータのcalendar.txtを読み込む
        using (StreamReader sr = new StreamReader("..\\..\\..\\..\\..\\home\\site\\wwwroot\\data\\GTFS-Data\\calendar.txt"))
        {
            //英語のカルチャー
            var culture = System.Globalization.CultureInfo.GetCultureInfo("en-US");
            //曜日取得
            string dayOfTheWeek = time.ToString("dddd", culture).ToLower();

            //1行読み込む
            line = sr.ReadLine();
            string[] str = line.Split(',');

            //回数を数える変数の初期化
            int count = 0;

            //判定時の曜日がGTFSデータのcalendar.txtの1行目のデータの何番目にあるか判定
            for(int i=0; i<str.Length; i++)
            {
                if(dayOfTheWeek.Equals(str[i]))
                {
                    count = i;
                    break;
                }
            }

            //初期化
            str = null;

            //GTFSデータのcalendar.txtの2行目から順に読み込む
            while ((line = sr.ReadLine()) != null)
            {
                str = line.Split(',');
                //判定した曜日の値が``1"とあるかどうか判定
                if (str[count]=="1")
                {
                    serviceId = str[0];
                    break;
                }
            }

        }

    }

}
 /// <summary>
  ///  路線IdとserviceIdと便情報からtripIdを得る
  /// <param name="routeId">GTFSデータのroute_id</param>
  /// <param name="serviceId">GTFSデータのserviceId</param>
  /// <param name="binNum">便番号</param>
  /// <param name="tripId">GTFSデータのtrip_id(出力)</param>
  /// </summary>
public static void getTripId(string routeId, string serviceId, int binNum, out string tripId)
{
    //初期化
    string line = "";
    ArrayList al = new ArrayList();
    tripId = null;

    //初期化
    string[] str = null;


    //GTFSデータのtrips.txtを読み込む
    using (StreamReader sr = new StreamReader("..\\..\\..\\..\\..\\home\\site\\wwwroot\\data\\GTFS-Data\\trips.txt"))
    {
        //1行ずつ読み込む
        while ((line = sr.ReadLine()) != null)
        {
            str = line.Split(',');
            //GTFSデータのroute_idとservice_idが一致するかどうか判定
            if((str[0].Equals(routeId)) && (str[1].Equals(serviceId)))
            {
                //route_idとservice_idが一致し，便番号が小さいものからアレイリストに格納
                //※この時，route_idとservice_idが一致するデータは運行する順番が早いものから順に並んでいる必要がある
                al.Add(line);
            }
        }

        //初期化
        str = null;

        //便番号に値するデータをstrに格納
        str = al[binNum - 1].ToString().Split(',');

        tripId = str[2];
    }


}


  /// <summary>
  ///  現在の運行情報を取得する関数
  ///  ※遅延時間を考慮していないため，今後考慮する必要あり
  /// <param name="tripId">GTFSのtrip_id</param>
  /// <param name="time">現在時刻</param>
  /// <param name="flag">運行情報の判定(出力)</param>
  /// </summary>
static void getServiceFlag(string tripId, DateTime time, out bool flag)
{
    //値の初期化
    string line = "";
    ArrayList al = new ArrayList();
    string[] str = null;
    flag = false;

    //GTFSデータのstop_times.txtを読み込む
    using (StreamReader sr = new StreamReader("..\\..\\..\\..\\..\\home\\site\\wwwroot\\data\\GTFS-Data\\stop_times.txt"))
    {
        //1行ずつ読み込む
        while ((line = sr.ReadLine()) != null)
        {
            str = line.Split(',');
            //GTFSのtrip_idが一致するか判定
            if (str[0].Equals(tripId))
            {
                //trip_idが一致したものをアレイリストに格納
                al.Add(line);
            }
        }
    }

    //値の初期化
    str = null;


    str = al[0].ToString().Split(',');

    //指定するtrip_idの便が出発する時間を取得
    DateTime startTime = DateTime.Parse(str[1]);

    //指定するtrip_idの便が最後のバス停に到着する時間を取得
    str = al[al.Count - 1].ToString().Split(',');
    DateTime endTime = DateTime.Parse(str[2]);

    //現在時刻が出発時間内か到着時間内か判定
    if((startTime <= time) && (time <= endTime))
    {
        //運行時間内であるため``true"フラグをつける
        flag = true;
    }

}

  /// <summary>
  ///  現在の指定する路線で運行しているバス停IDを取得する関数
  /// <param name="tripId">GTFSのtrip_id</param>
  /// <param name="busStopIds">バス停ID(出力)</param>
  /// </summary>
public static void getBusStopIds(string tripId, out string[] busStopIds)
{
    ArrayList al = new ArrayList();
    string line = "";
    string[] str = null;

    //GTFSデータのstop_times.txtを読み込む
    using (StreamReader sr = new StreamReader("..\\..\\..\\..\\..\\home\\site\\wwwroot\\data\\GTFS-Data\\stop_times.txt"))
    {
        //1行目のから読み
        sr.ReadLine();

        //1行ずつ読み込む
        while ((line = sr.ReadLine()) != null)
        {
            str = line.Split(',');
            //GTFSのtrip_idが一致するか判定
            if (str[0].Equals(tripId))
            {
                al.Add(str[3]);
            }
        }

    }

    busStopIds = (String[]) al.ToArray( typeof( string ) );
}
public static async Task inputBusLocationTableMethod(ILogger log, ICollector <BusLocationTableLPWA> outputBusLocationTable, CloudTable inputBusLocationTable, string routeKey, DateTime time, double absoluteBusLat, double absoluteBusLon, string tripId, int serviceNum, bool serviceFlag, int approach, DateTime timeBefore)
{
    DateTime dt = DateTime.Now;
    string todayDate = dt.ToString("yyyyMMdd");

    int count = 0;

    //クエリのフィルター（最新情報を取得）
    string BusLocationTablefilter1 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, routeKey);
    string BusLocationTablefilter2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, tripId);
    string BusLocationTablefilter3 = TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, timeBefore);

    string filterRange = TableQuery.CombineFilters(BusLocationTablefilter2, TableOperators.And, BusLocationTablefilter3);

    //クエリ
    TableQuery<BusLocationTableEntity> rangeQuery = new TableQuery<BusLocationTableEntity>().Where(
        TableQuery.CombineFilters(BusLocationTablefilter1, TableOperators.And, filterRange)
    );

    foreach (BusLocationTableEntity entityLocation in
    await inputBusLocationTable.ExecuteQuerySegmentedAsync(rangeQuery, null))
    {

        //指定したtrip_idを持つ情報の数を数える
        string[] str = entityLocation.RowKey.Split('_');
        
        if(str[0].Equals(tripId))
        {
            if(count < int.Parse(str[1]))
            {
                count = int.Parse(str[1]);
            }    
        }
    }
    
    count = count + 1;

    string rowKey = tripId + "_" + count.ToString("D4") + "_" + todayDate;
    log.LogInformation($"Table_RowKey:{rowKey}");

    //BusLocationTableに情報を格納
    outputBusLocationTable.Add(
        new BusLocationTableLPWA() {　　　　　
            PartitionKey = routeKey,
            RowKey = rowKey,
            Latitude = absoluteBusLat,
            Longitude = absoluteBusLon,
            ServiceFlag = serviceFlag,
            ServiceNum = serviceNum,
            Time = time,
            approachStatsu = approach

        }
    );

    log.LogInformation($"{routeKey}:Data input BuslocationTableLPWA");
}
  /// <summary>
  ///  遅延時間などをテーブル（busDelayTable）に格納する関数
  /// <param name="outputBusDelaynTable">遅延時間などをを格納するテーブルのインスタンス</param>
　/// <param name="inputBusDelayTable">遅延時間などを格納するテーブルのクラウドテーブル型のインスタンス</param>
　/// <param name="log"></param>
　/// <param name="routeKey">路線を示すKey</param>
  /// <param name="tripId">GTFSデータのtrip_id</param>
　/// <param name="time">位置情報を取得した時刻</param>
  /// <param name="serviceNum">バスの便情報</param>
  /// <param name="delayTime">バスの遅延時間</param>
  /// <param name="stopSequence">バスのシーケンス番号
  /// <param name="timeBefore">少し前の時刻
  /// </summary>
public static async Task inputBusDelayTableMethod(ICollector <BusDelayTableLPWA> outputBusDelayTable, CloudTable inputBusDelayTable, ILogger log, string busStopId, string routeKey, string tripId,  DateTime time, int serviceNum, int delayTime, int stopSequence,int approachStatus1,int approachStatus2, DateTime timeBefore)
{
    DateTime dt = DateTime.Now;
    string todayDate = dt.ToString("yyyyMMdd");

    //クエリのフィルター
    string BusDelayTablefilter1 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, routeKey);
    string BusDelayTablefilter2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, tripId);
    string BusDelayTablefilter3 = TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, timeBefore);

    string filterRange2 = TableQuery.CombineFilters(BusDelayTablefilter2, TableOperators.And, BusDelayTablefilter3);

    //指定のtripIdのテーブルに格納されているデータの数
    int count = 0;

    //最新の到着判定をしたバス停Id（初期値として"9999-1"を入れておく）
    string previousBusStopId = "9999-1";

    //クエリ
    TableQuery<BusDelayTableEntity> rangeQuery2 = new TableQuery<BusDelayTableEntity>().Where(
        TableQuery.CombineFilters(BusDelayTablefilter1, TableOperators.And, filterRange2)
    );

    
    foreach (BusDelayTableEntity entityDelay in
    await inputBusDelayTable.ExecuteQuerySegmentedAsync(rangeQuery2, null))
    {

        //指定したtrip_idを持つ情報の数を数える
        string[] str = entityDelay.RowKey.Split('_');
        if(str[0].Equals(tripId))
        {
            if(count < int.Parse(str[1]))
            {
                count = int.Parse(str[1]);
                previousBusStopId = entityDelay.StopId;
            }    
        }
            
    }


    //同じバス停との到着判定をしていないか判定
    if(!(previousBusStopId.Equals(busStopId)))
    {
        count = count + 1;

        //BusDelayTableのRowKey
        string rowKey = tripId + "_" + count.ToString("D4") + "_" + todayDate;
        log.LogInformation($"Table_RowKey{rowKey}");

        //BusDelayTableに情報を格納
        outputBusDelayTable.Add(
            new BusDelayTableLPWA() {　　　　　
                PartitionKey = routeKey,
                RowKey = rowKey,
                DelayTime = delayTime,
                ReasonId = 1,
                ServiceNum = serviceNum,
                StopId = busStopId,
                StopSequence = stopSequence,
                Time = time,
                approachStatus1 = approachStatus1,
                approachStatus2 = approachStatus2
            }
        );

        log.LogInformation($"{routeKey} data input BusDelayTableLPWA");
    }

    //最初のバス停かどうか判定
    else if(stopSequence == 0)
    {
        count = count + 1;

        //BusDelayTableのRowKey
        string rowKey = tripId + "_" + count.ToString("D4") + "_" + todayDate;


        //BusDelayTableに情報を格納
        outputBusDelayTable.Add(
            new BusDelayTableLPWA() {　　　　　
                PartitionKey = routeKey,
                RowKey = rowKey,
                DelayTime = delayTime,
                ReasonId = 1,
                ServiceNum = serviceNum,
                StopId = busStopId,
                StopSequence = stopSequence,
                Time = time,
                approachStatus1 = approachStatus1,
                approachStatus2 = approachStatus2
            }
        );
        log.LogInformation($"{routeKey} data input BusDelayTableLPWA");
    }    
}

// <sumary>
//バッテリ情報をテーブル（BatteryInformationTable）に格納する関数
/// <param name="outputBatteryInformationTable">バッテリ情報を格納するテーブルのインスタンス</param>
/// <param name="log"></param>
/// <param name="TableContents">テーブル内容を示す</param>
/// <param name="BusStopId">バス停ID</param>
/// <param name="Time">電圧を取得した時刻</param>
/// <param name="Voltage">バス停のバッテリ電圧残量</param>
/// <param name="BatteryLevel">バス停のバッテリレベル</param>
/// </summary>
public static async Task insertBatteryInformationTableMethod(ILogger log, ICollector <BatteryInformationTable> outputBatteryInformationTable, string BusStopId, string Time, double Voltage, int BatteryLevel){

    string TableContents = "BatteryInfo";
    DateTime Dt = DateTime.Now;
        //クエリのフィルター（最新情報を取得）
    string BatteryInformationTablefilter1 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, TableContents);
    string BatteryInformationTablefilter2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, Time);
    string BatteryInformationTablefilter3 = TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, Dt);

    string filterRange3 = TableQuery.CombineFilters(BatteryInformationTablefilter2, TableOperators.And, BatteryInformationTablefilter3);

    //クエリ
    TableQuery<BatteryInformationTableEntity> rangeQuery = new TableQuery<BatteryInformationTableEntity>().Where(
        TableQuery.CombineFilters(BatteryInformationTablefilter1, TableOperators.And, filterRange3)
    );

        //BatteryInformationTableに情報を格納
    outputBatteryInformationTable.Add(
        new BatteryInformationTable() {　　　　　
            PartitionKey = TableContents,
            RowKey = Time,
            BusStopId = BusStopId,
            Voltage = Voltage,
            BatteryLevel = BatteryLevel
        }
    );
    log.LogInformation($"{BusStopId} data insert BatteryInformationTable");
}

// <sumary>
//アラート管理情報をテーブル（BatteryInformationTable）に格納する関数
/// <param name="outputBatteryInformationTable">アラート管理情報を格納するテーブルのインスタンス</param>
/// <param name="log"></param>
/// <param name="TableContents">テーブル内容を示す</param>
/// <param name="BusStopId">バス停ID</param>
/// <param name="Time">バス停のバッテリ電圧残量を取得した時刻</param>
/// <param name="AlertLevel">バス停のバッテリ交換を促すアラートレベル</param>
/// </summary>
/*public static async Task insertAlertManagementTableMethod(ILogger log, ICollector <AlertManagementTable> outputAlertManagementTable, string BusStopId, string Time, int BatteryLevel){

    string TableContents = "Alert";
    DateTime Dt = DateTime.Now;
    int AlertLevel = 0;
        //クエリのフィルター（最新情報を取得）
    string AlertManagementTablefilter1 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, TableContents);
    string AlertManagementTablefilter2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, Time);
    string AlertManagementTablefilter3 = TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, Dt);

    string filterRange4 = TableQuery.CombineFilters(AlertManagementTablefilter2, TableOperators.And, AlertManagementTablefilter3);

    //クエリ
    TableQuery<AlertManagementTableEntity> rangeQuery = new TableQuery<AlertManagementTableEntity>().Where(
        TableQuery.CombineFilters(AlertManagementTablefilter1, TableOperators.And, filterRange4)
    );

    //アラートレベルを設定
    if(BatteryLevel == 0){
        AlertLevel = 2;
    }
    else{
        AlertLevel = 1;
    }
        //AlertManagementTableに情報を格納
    outputAlertManagementTable.Add(
        new AlertManagementTable() {　　　　　
            PartitionKey = TableContents,
            RowKey = Time,
            BusStopId = BusStopId,
            AlertLevel = AlertLevel
        }
    );
    log.LogInformation($"{BusStopId} data insert AlertManagementTable");
}*/


// <sumary>
// 運行終了メッセージをバス停に送る
// <param name="sequenceNum">次バス停のシーケンス番号</param>
// <param name="routeId">ルートID</param>
// <param name="busStopId">バス停ID</param>
// </summary>
public static void sendFinish(int sequenceNum, int routeId, string deviceId, out string result){
    // 送信メッセージ作成
    result = null;
    string message = null;
    string sendInformationType = Convert.ToString(3, 2).PadLeft(4, '0');
    string sendContentType = Convert.ToString(6, 2).PadLeft(4, '0');
    // string binary = (sendInformationType + sendContentType).PadRight(56,'0');
    string binary = sendInformationType + sendContentType;
    //送信する16進数データ
    message = Convert.ToString(Convert.ToInt64(binary, 2), 16);

    // ダウンリンクメッセージを送信するためのトークンを取得
    string accessToken = null;
    getAccessToken(out accessToken);
    string[] strToken = accessToken.Split(',');
    //アクセストークン取得可能か判定（正しくアクセストークンを取得できたら第0要素にアクセストークンが格納されている）
    if(strToken[1] == ""){
        //アクセストークンを取得
        accessToken = strToken[0];

        //ダウンリンクメッセージ送信用URL
        string downLinkSendUrl = "https://dx-api.thingpark.com/core/latest/api/devices/" + deviceId + "/downlinkMessages";
        
        try{
            using (WebClient webClient = new WebClient()){
                //メッセージを送信する際のポストデータ
                string postDataGetResult = "{ \"payloadHex\":\"" + message + "\", \"targetPorts\":\"1\"}";

                //付属するヘッダ
                webClient.Headers.Add("Content-Type: application/json");
                webClient.Headers.Add("Accept: application/json");
                webClient.Headers.Add("Authorization: Bearer " + accessToken);

                //メッセージ送信の際の応答メッセージ
                result = webClient.UploadString(downLinkSendUrl, postDataGetResult);
            }
        }
        catch (Exception e){
            //continue;
            result = e.Message;
        }
    }

}

// <summary>
// ダウンリンク通信に用いるAPIを利用する際のトークンを取得する関数
// <param name="token">取得したトークン(出力)</param>
// </summary>
public static void getAccessToken(out string accessToken){
    //  初期化
    accessToken = null;
    // 認証処理用URL
    string certificationUrl = "https://dx-api.thingpark.com/admin/latest/api/oauth/token";
    // アカウント情報
    string mailAddress = GetEnvironmentVariable("ThingParkWirelessUserID");
    string password = GetEnvironmentVariable("ThingParkWirelessPassword");

    try{
        using (WebClient webClient = new WebClient()){
            //トークンを取得する際のポストデータ
            string postDataGetToken = "grant_type=client_credentials&client_id=macnicapoc-api/" + mailAddress + "&client_secret=" + password;
            //付属するヘッダ
            webClient.Headers.Add("Content-Type:application/x-www-form-urlencoded");
            webClient.Headers.Add("Accept:application/json");

            //トークン取得の際の応答メッセージ
            string resText = webClient.UploadString(certificationUrl, postDataGetToken);

            //Jsonファイルにシリアライズ
            var certificationJson = DynamicJson.Parse(resText);

            //アクセストークンをjsonから取り出す
            accessToken = certificationJson.access_token.ToString() + ",";
        }
    }
    catch (Exception e){
        accessToken = "," + e.Message;
    }
}

// <sumary>
// 時刻同期メッセージをバス停に送る
// <param name="">次バス停のシーケンス番号</param>
// </summary>
public static void sendUnixTime(string devEUI, DateTime dateTime, out string result,ILogger log){
    // 現在日時をUTC時間に変換
    dateTime = dateTime.ToUniversalTime();
    log.LogInformation($"dateTime{dateTime}");
    // UnixTime取得
    int unixTime = (int)(dateTime.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
    log.LogInformation($"unixTime{unixTime}");
    // 送信メッセージ作成
    result = null;
    string message = null;
    string sendInformationType = Convert.ToString(7, 2).PadLeft(4, '0');
    string sendContentType = Convert.ToString(3, 2).PadLeft(4, '0');
    string sendUnixTime = Convert.ToString(unixTime, 2).PadLeft(32, '0');
    string binary = sendInformationType + sendContentType + sendUnixTime;
    // 送信する16進数データ
    message = Convert.ToString(Convert.ToInt64(binary, 2), 16);
    log.LogInformation($"msg:{message}");
    // ダウンリンクメッセージを送信するためのトークンを取得
    string accessToken = null;
    DateTime dt_getTokenStart = DateTime.Now;
    getAccessToken(out accessToken);
    DateTime dt_getTokenEnd = DateTime.Now;
    log.LogInformation($"Get AccessToken Time{dt_getTokenEnd-dt_getTokenStart}");
    string[] strToken = accessToken.Split(',');
    log.LogInformation($"AccessToken{accessToken}");
    //アクセストークン取得可能か判定（正しくアクセストークンを取得できたら第0要素にアクセストークンが格納されている）
    if(strToken[1] == ""){
        //アクセストークンを取得
        accessToken = strToken[0];

        // devEUI = "000B78FFFE051153";

        //ダウンリンクメッセージ送信用URL
        string downLinkSendUrl = "https://dx-api.thingpark.com/core/latest/api/devices/" + devEUI + "/downlinkMessages";
        
        try{
            using (WebClient webClient = new WebClient()){
                //メッセージを送信する際のポストデータ
                string postDataGetResult = "{ \"payloadHex\":\"" + message + "\", \"targetPorts\":\"15\"}";

                //付属するヘッダ
                webClient.Headers.Add("Content-Type: application/json");
                webClient.Headers.Add("Accept: application/json");
                webClient.Headers.Add("Authorization: Bearer " + accessToken);

                //メッセージ送信の際の応答メッセージ
                DateTime dt_responseStart = DateTime.Now;
                log.LogInformation($"Response Start Time{dt_responseStart}");
                result = webClient.UploadString(downLinkSendUrl, postDataGetResult);
            }
        }
        catch (Exception e){
            //continue;
            result = e.Message;
        }
    }
}

public class BusLocationTableLPWA
{
    public string PartitionKey { get; set; }
    public string RowKey { get; set;  }
    public double Latitude {  get; set; }
    public double Longitude { get; set; } 
    public bool ServiceFlag {get; set;}
    public int ServiceNum { get; set; }
    public DateTime Time { get; set; }
    public int approachStatsu { get; set; }
}

public class BusDelayTableLPWA
{
    public string PartitionKey { get; set; }
    public string RowKey { get; set;  }
    public int DelayTime { get; set;  }
    public int ReasonId { get; set;  }
    public int ServiceNum { get; set; }
    public string StopId {get; set;}
    public int StopSequence {get; set;}
    public DateTime Time { get; set;  }
    public int approachStatus1 { get; set;  }
    public int approachStatus2 { get; set;  }

}

public class BatteryInformationTable
{
    public string PartitionKey { get; set; }
    public string RowKey { get; set; }
    public double Voltage { get; set; }
    public int BatteryLevel { get; set; }
    public string BusStopId { get; set; }
}

public class AlertManagementTable
{
    public string PartitionKey { get; set; }
    public string RowKey { get; set; }
    public int AlertLevel { get; set; }
    public string BusStopId { get; set; }
}

public class BusDelayTableEntity : TableEntity
{
    public int DelayTime {get; set;}
    public int ReasonId {get; set;}
    public int ServiceNum {get; set;}
    public DateTime Time {get; set;}
    public string StopId {get; set;}
    public int StopSequence {get; set;}
}

public class BusLocationTableEntity : TableEntity
{
    public double Latitude {get; set;}
    public double Longitude {get; set;}
    public DateTime Time {get; set;}
    public double Speed {get; set;}
    public int serviceNum {get; set;}
    public int approach {get; set;}
}

public class BatteryInformationTableEntity : TableEntity
{
    public string Time { get; set; }
    public double Voltage { get; set; }
    public int BatteryLevel { get; set; }
    public string BusStopId { get; set; }
}

public class AlertManagementTableEntity : TableEntity
{
    public string Time { get; set; }
    public int AlertLevel { get; set; }
    public string BusStopId { get; set; }
}

public class BusStopTableEntity : TableEntity
{
    public string DeviceId {get; set;}
    public double Latitude {get; set;}
    public double Longitude {get; set;}

}

// LastRouteTableから取り出す項目の選択
public class LastRouteTableEntity : TableEntity
{
    public string DeviceID {get; set;}
    public int RouteID {get; set;}
    public int SequenceNumber {get; set;}
    public string StationName {get; set;}
}