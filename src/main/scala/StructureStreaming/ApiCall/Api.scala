package StructureStreaming.ApiCall


import java.net.URI

import com.google.gson.Gson
import com.typesafe.config.ConfigFactory
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.impl.client.{BasicResponseHandler, DefaultHttpClient, HttpClientBuilder}

import scala.collection.JavaConverters._
import net.liftweb._
import org.apache.http.entity.StringEntity


case class cl_array(id: String,first_name: String,last_name: String,avatar: String)
case class cl(total_pages: String, data:Array[cl_array],page:String,total:String,per_page:String)

case class write_post(name:String,Job:String)

case class Token(access_token: String, instance_url: String,
                 id: String,
                 token_type: String,
                 issued_at: String,
                 signature: String)

class Util {

  def getAccessToken() : String = {

    val login = "https://login.salesforce.com/services/oauth2/token"
    var access_token = ""
    try {
      val conf = ConfigFactory.load()
      val UserName = conf.getString("force.UserName")
      val PassWord     = conf.getString("force.PassWord")
      val LoginURL     = conf.getString("force.LoginURL")
      val GrantService = conf.getString("force.GrantService")
      val ClientID     = conf.getString("force.ClientID")
      val ClientSecret = conf.getString("force.ClientSecret")

      val loginURL = LoginURL +
        GrantService +
        "&client_id=" + ClientID +
        "&client_secret=" + ClientSecret +
        "&username=" + UserName +
        "&password=" + PassWord

      val client = new DefaultHttpClient
      val post = new HttpPost(loginURL)
      val handler = new BasicResponseHandler();
      val response = client.execute(post)
      println("response:" +  response)
      val body = handler.handleResponse(response);
      println(response)
      val gson = new Gson
      val jsonObject = gson.fromJson(body, classOf[Token])
      access_token = jsonObject.access_token
      println("access_token: " + access_token)


    } catch {
      case ioe: java.io.IOException =>
      case ste: java.net.SocketTimeoutException =>
    }
    return access_token

  }
}

object Api {


  def main(args: Array[String]): Unit = {

    println(getRestContent("https://reqres.in//api/users"))
    val str=getRestContent("https://reqres.in//api/users")
    val js="""{"page":1,"per_page":3,"total":12,"total_pages":4,"data":[{"id":1,"first_name":"George","last_name":"Bluth","avatar":"https://s3.amazonaws.com/uifaces/faces/twitter/calebogden/128.jpg"},{"id":2,"first_name":"Janet","last_name":"Weaver","avatar":"https://s3.amazonaws.com/uifaces/faces/twitter/josephstein/128.jpg"},{"id":3,"first_name":"Emma","last_name":"Wong","avatar":"https://s3.amazonaws.com/uifaces/faces/twitter/olegpogodaev/128.jpg"}]}"""

   // val json1:Option[Any] = JSON.parseFull(js)
    //val map:Map[String,Any] = json1.get.asInstanceOf[Map[String, Any]]

    implicit val formats = json.DefaultFormats
    val content = json.parse(str)
    val config = content.extract[cl]

    //val p=write_post(config.)

    config.data.foreach {p=>
      val data=new write_post(p.last_name,p.first_name)
      println(data)
      val spockAsJson = new Gson().toJson(data)
      println(spockAsJson)
      postRestContent(spockAsJson)

    }

  }



  def postRestContent(url:String): Unit = {
    val uri = new URI("https://reqres.in/api/users")
    val post = new HttpPost(uri)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(url))
    val response = (new DefaultHttpClient).execute(post)
    println(response)

  }

  def getRestContent(url:String): String = {
    val httpClient = new DefaultHttpClient()
    val httpResponse = httpClient.execute(new HttpGet(url))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    httpClient.getConnectionManager().shutdown()
    return content
  }

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
}

