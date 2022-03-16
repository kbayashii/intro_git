#r "Newtonsoft.Json"
#r "System.Configuration"
#r "System.Data"

//email
using Microsoft.Extensions.Logging;

//encoding
using System.Text;

using System.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;

//http post
using Newtonsoft.Json.Linq;
using System.Net.Http.Headers;
using System.Collections.Generic;
using System.Runtime.Serialization.Json;
using System.IO;
using static System.Console;
using System.Runtime.Serialization.Json;
using System.Runtime.Serialization;

private static readonly HttpClient client = new HttpClient();

public static async Task<IActionResult> Run(HttpRequest req, ILogger log)
{
    log.LogInformation("C# HTTP trigger function processed a request.");

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
    
    //payloadから2つのメッセージ識別子を取得
    string informationType = payload.Substring(0,1);
    string contentType = payload.Substring(1,1);

    string msg = null;
    string receivedMessageBinary = null;

    if(informationType == "2" && contentType == "1"){
        //受信した16進表記の文字列を2進表記の文字列に変換
        for(int i = 0; i < payload.Length; i++){
            string tmpString = Convert.ToString(payload[i]);
            string tmpBinary = Convert.ToString(Convert.ToInt64(tmpString, 16), 2).PadLeft(4, '0');
            // log.LogInformation($"tmpBinary:{tmpBinary}");
            receivedMessageBinary = receivedMessageBinary + tmpBinary;
            // log.LogInformation($"receivedMessageBinary:{receivedMessageBinary}");
        }
    
        int BatteryLevel = Convert.ToInt32(receivedMessageBinary.Substring(40,4),2);
        if(BatteryLevel >= 2){
            log.LogInformation($"no alert");
                    }
        else{
                if(BatteryLevel == 1){
                        msg = "バッテリーが少なくなっています";
                }else{
                    msg = "バッテリーを交換してください";
                    }
            }
    
            //Push message through Slack
            using (var client = new HttpClient()) 
            { 
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                client.DefaultRequestHeaders.Add("User-Agent", "AzureFunctions");

                //Copy paste uri from Slack
                var uri = "https://hooks.slack.com/services/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";//Slack's webhook URL
                // emoji list - http://www.emoji-cheat-sheet.com/
                var send_msg = new SlackHook {text = msg, icon_emoji = ":beer:"};

                StringContent SlackMsg = new StringContent(JsonConvert.SerializeObject(send_msg));
                HttpResponseMessage response = client.PostAsync(uri,SlackMsg).Result; //post to slack
            }
    }else{
        log.LogInformation($"no alert");
    }

    return DevEUI != null
        ? (ActionResult)new OkObjectResult($"Received from, {DevEUI}")
        : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
}

public class SlackHook
{
    public string text {get;set;}
    public string icon_emoji {get;set;}
}