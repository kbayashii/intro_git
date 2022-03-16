#r "Newtonsoft.Json"

using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;
using System;
using Newtonsoft.Json.Linq;

//受け取ったHTTP要求からデータを切り出し，どのようなデータか識別して対応するEventHubに渡す
public static async Task Run(HttpRequest req,ICollector<string> outputEventHubMessage, ILogger log)
{
    //LoRaデバイスからHTTP要求で受け取ったデータを全格納
    string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

    //JSON文字列をJson.NETのオブジェクトに変換
    JObject receiceDataJson = JObject.Parse(requestBody);
    log.LogInformation($"receiveData{receiceDataJson}");

    //ペイロードを取得
    string payload =  receiceDataJson["DevEUI_uplink"]["payload_hex"].ToString();
    log.LogInformation($"payload:{payload}");

    //デバイスIDを取得
    string DevEUI =  receiceDataJson["DevEUI_uplink"]["DevEUI"].ToString();
    log.LogInformation($"DevEUI:{DevEUI}");
    
    //ここから識別子によりデータの識別をし，適したEventHubにデータを渡す処理
    //payloadから2つのメッセージ識別子を取得
    string informationType = payload.Substring(0,1);
    string contentType = payload.Substring(1,1);

    //informationTypeによるメッセージ識別
    switch(informationType){
        //バスの位置情報
        case "1":
            log.LogInformation("received bus location information");
            outputEventHubMessage.Add(payload);
            break;
        
        //バス停に関係する情報
        case "2":
            log.LogInformation("received bus stop information");
            //contentTypeによるメッセージ識別
            switch(contentType){
                //バッテリ残量メッセージ
                case "1":
                    log.LogInformation("received battery information");
                    outputEventHubMessage.Add(payload);
                    break;

                default:
                    log.LogError($"received invaild value of contentType !! (informationType == {informationType},contentType == {contentType},DevEUI=={DevEUI})");
                    break;              
            }

            break;

        //バスの運行情報
        case "3":
            log.LogInformation("received bus operation information");
            switch(contentType){
                case "3":
                    log.LogInformation("received bus banner ack");
                    payload += DevEUI;
                    outputEventHubMessage.Add(payload);
                    break;                        
            }
            break;

        //災害情報
        case "4":
            log.LogInformation("received disaster information");
            break;

        //公共情報
        case "5":
            log.LogInformation("received civil information");
            break;

        //コンテンツ切り替え情報
        case "6":
            log.LogInformation("received contents change information");
            switch(contentType){
                case "2":
                    log.LogInformation("received current content information");
                    payload += DevEUI;
                    outputEventHubMessage.Add(payload);
                    break;
            }
            break;
        
        //システム情報
        case "7":
            log.LogInformation("received system information");
            switch(contentType){
                case "1":
                    log.LogInformation("received maintainance information");
                    break;
                
                //現在時刻要求メッセージ
                case "2":
                    log.LogInformation("received current time request information");
                    payload += DevEUI;
                    outputEventHubMessage.Add(payload);
                    break;
                
                //現在時刻応答メッセージ
                case "3":
                    log.LogInformation("received current time response information");
                    break; 

                default:
                    log.LogError($"received invaild value of contentType !! (informationType == {informationType},contentType == {contentType},DevEUI=={DevEUI})");        
                    break;
            }
            break;

        //1~7以外の値が入力されたとき    
        default:
            log.LogError($"received invaild value of informationType !! (informationType == {informationType},contentType == {contentType},DevEUI=={DevEUI})");
            break;
    }
    //ここまで識別子によりデータの識別をし，適したEventHubにデータを渡す処理
}