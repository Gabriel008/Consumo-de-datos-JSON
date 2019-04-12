/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pegasus;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLConnection;
import java.security.AccessControlException;
import java.security.cert.X509Certificate;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.nio.charset.Charset;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.json.JSONArray;
import org.json.JSONStringer;


/**
 *
 * @author Gabriel
 */
public class Pegasus {
    /**
     * @param args the command line arguments
     */
    static String username="cruiz@micrologica.com";
    static String password="pcontador"; 
    static String source="https://pro.analyzegps.cl/api";
    static String Token;
    static String urlToken;
    static String Tramas="";
    static String Trama="";
    //static int n=0;
    
    static int Tipo;
    static int Code;
    static String Lat1;
    static String Lon1;
    static long Lat;
    static long Lon;
    static String Veh = "";
    static String Fecha = "";
    static String Hora = "";
    static int Vel;
    static int Dir;
    
    static String id_veh = "";
    static String codreg = "";
    static String LatStr,LatAux = "";
    static String LonStr,LonAux = "";
    static Statement st;
    static ResultSet rs,rs1,rs2;
    static String ZonaHoraria = "";
    static String FechaRx = "";
    static String FechaAux = "";
    static String CodLugar = "";
    static boolean registros=true;
    static boolean rest=false;
    static String fields="";
    static String [] Argumentos;
    static String Datos="";
    static boolean Enviar=false;
    static JSONObject jsonObject = null;
    
   

public static void main(String[] args) 
{
        // TODO code application logic here
                Argumentos=args;
                 for(int i=1;i<args.length;i++)
                 {
                    System.out.println("Argumento "+i+": "+args[i]);
                    fields = fields +args[i] +",";
                 }
    
    try
    {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection conn = null;

        while (true)
        {
            try
            {
                //conn = DriverManager.getConnection("jdbc:sqlserver://150.0.20.202;databaseName=DB_GPS","sa","Micrologica2014");
                conn = DriverManager.getConnection("jdbc:sqlserver://;servername=localhost\\SQLEXPRESS;databaseName=DB_GPS","sa","Micrologica2014");
                
            }
            catch (SQLException e)
            {
                e.printStackTrace();
                Log.log(e.toString());
                if (conn!=null)
                conn.close();
                int c = 0;
                while (c==0)
                {
                    try
                    {
                    //conn = DriverManager.getConnection("jdbc:sqlserver://150.0.20.202;databaseName=DB_GPS","sa","Micrologica2014");
                        conn = DriverManager.getConnection("jdbc:sqlserver://;servername=localhost\\SQLEXPRESS;databaseName=DB_GPS","sa","Micrologica2014");
                        Log.log("Se recupera conexion");
                        c=1;
                    }
                    catch (SQLException s)
                    {
                        System.out.println("Err: "+s.toString());
                    }
                    Thread.sleep(60000);
                }
            }
            try
            {
                String url = source+"/login";
		URL obj = new URL(url);
		HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
                //add reuqest header
		con.setRequestMethod("POST");
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
                String urlParameters = "username=cruiz@micrologica.com&password=pcontador";
		//Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) 
                    {
			response.append(inputLine);
                    }
		in.close();
                //Token
                JSONObject json = new JSONObject(response.toString());
		Token= json.getString("auth");
                //System.out.println("Token: "+Token);
                Log.log("Conexión a URL con Autentificacion Token");
                //sendGet(Token,conn,args);
                sendGet(conn);
                
                Log.log("Fin lectura");
               
                Tramas="";
                Log.log("Se cierra conexion"+"\n");
                conn.close();
                Thread.sleep(60000);
            }
            catch (IOException r)
            {
                System.out.println("IO Err");
                Log.log("IO Err: "+r.toString()+"\n");
                r.printStackTrace();
                Thread.sleep(60000);
            }
            catch (AccessControlException e)
            {
                System.out.println("AC Err");
                Log.log("AC Err: "+e.toString()+"\n");
                e.printStackTrace();
            }
            catch (JSONException j)
            {
                System.out.println("JS Err");
                Log.log("JS Err: "+j.toString()+"\n");
                j.printStackTrace();
            }
            
            catch (SQLException s)
            {
                System.out.println("SQ Err");
                Log.log("SQ Err: "+s.toString()+"\n");
                s.printStackTrace();
                conn.close();
            }
        }
    }
    catch(Exception e)
    {
        System.out.println("OT Err");
        Log.log("OT Err: "+e.toString()+"\n");
        e.printStackTrace();
    }
    
}
    

public static void sendGet(Connection conn) throws Exception 
{
    System.out.println("Token: "+Token+"\n");

    if (Token !=null)
    {
        String url1 = source+"/vehicles"; 
        URL obj = new URL(url1);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        // optional default is GET
        con.setRequestMethod("GET");
        //add request header
        con.setRequestProperty("Authenticate", Token);
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();
        while ((inputLine = in.readLine()) != null) 
            {
                response.append(inputLine);
            }
        in.close();
        Log.log("Conexión a URL vehicles");
        JSONObject json = new JSONObject(response.toString());
        JSONArray values = json.getJSONArray("data");
        for (int i = 0; i < values.length(); i++) 
            {  
                JSONObject data = values.getJSONObject(i); 
                Integer id = data.getInt("id");
                String name = data.getString("name");
              
                populateTable(id,name,i,conn);
            }
    }
}
    
 
public static void populateTable(int id,String name,int i,Connection conn)throws Exception{
            if (Token!=null)
            {
                
            Calendar c1 = GregorianCalendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            c1.add(Calendar.DATE, -7);

                
                String url = "https://pro.analyzegps.cl/api/rawdata?vehicles="+id+"&tail="+Argumentos[0]+"&order=+event_time&fields="+fields.substring(0, fields.length()-1)+"&from="+sdf.format(c1.getTime());
                //String url = "https://pro.analyzegps.cl/api/rawdata?vehicles="+id+"&head="+Argumentos[0]+"&order=+event_time&fields="+fields.substring(0, fields.length()-1)+"&from=2016-09-06T15%3A10%3A00";
                //String url = "https://pro.analyzegps.cl/api/rawdata?vehicles="+id+"&head="+Argumentos[0]+"&order=+event_time&fields="+fields.substring(0, fields.length()-1)+"&from=2016-08-29T17%3A30%3A00";
                URL obj = new URL(url);
                
                
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");
             
                //add request header
		con.setRequestProperty("Authenticate", Token);
                             
		int responseCode = con.getResponseCode();
		System.out.println("Sending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);
                Log.log("Conexión a URL con registros de posición");
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                
               // BufferedReader in = new BufferedReader(new InputStreamReader(((HttpURLConnection) (new URL(url)).openConnection()).getInputStream(), Charset.forName("UTF-8")));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		//print result
                if(!(response.toString().equals("{\"units\": {\"volume\": \"liter\", \"distance\": \"meter\", \"speed\": \"kph\", \"time\": \"second\"}, \"events\": []}")))
                { Trama=response.toString();
                  
                }
                 System.out.println(response.toString()+"\n");
                   //System.out.println("Tramas : "+Tramas);
                Log.log("Lectura de trama vehiculo : "+name+" numero de trama: "+(i+1));
                jsonProcess(id, name, conn);
            }
         }


    public static void jsonProcess(int ID, String name, Connection conn) throws Exception
    {  
        String head="";
        String code = "";
        String hdop ="";
        String event_time = "";
        String type = "";
        String vid = "";
        String lon = "";
        String sv = "";
        String mph ="";
        String label = "";
        String valid_position = "";
        String lat = "";
        String system_time = "";
        String speed = "";
        String id = "";
        String device_id = "";
        
        if(!(Trama.equals("")))
        {
            String jsonText = Trama;
            JSONObject json = new JSONObject(jsonText);
            JSONArray values = json.getJSONArray("events");

            
        for (int i = 0; i < values.length(); i++){  
          //  System.out.println(" I : "+i);
            
             for(int x=1;x<Argumentos.length;x++) {
                    JSONObject events = values.getJSONObject(i); 
                      //   System.out.println(" X : "+x);
                if(String.valueOf(events.get("head")).equals("null"))
                    {head ="0.0";}
                else
                    {head = String.valueOf(events.getDouble("head"));}

                if(String.valueOf(events.get("hdop")).equals("null") )
                    {hdop ="0.0";}
                else
                    {hdop = String.valueOf(events.getDouble("hdop"));}

                if(events.get("event_time").equals("null"))
                    {event_time ="";}
                else
                    {event_time = events.getString("event_time");}

    if(String.valueOf(events.get("type")).equals("null"))
        {type ="10.0";}
    else
        {type = String.valueOf(events.getDouble("type"));}

    if(String.valueOf(events.get("code")).equals("null"))
        {code ="0.0";}
    else
        {code = String.valueOf(events.getDouble("code"));}

                if(String.valueOf(events.get("vid")).equals("null"))
                    {vid ="0.0";}
                else
                    {vid = String.valueOf(events.getDouble("vid"));}

                if(String.valueOf(events.get("lon")).equals("null")||String.valueOf(events.get("lon")).equals("0.0"))
                    {lon ="7700000000";}
                else
                    {lon = String.valueOf(events.getDouble("lon"));}

                if(String.valueOf(events.get("sv")).equals("null"))
                    {sv ="0.0";}
                else
                    {sv = String.valueOf(events.getDouble("sv"));}

                if(String.valueOf(events.get("mph")).equals("null"))
                    {mph ="0.0";}
                else
                    {mph = String.valueOf(events.getDouble("mph"));}

                if(events.get("label").equals("null"))
                    {label ="";}
                else
                    {label = events.getString("label");}


                 if(String.valueOf(events.get("lat")).equals("null")||String.valueOf(events.get("lat")).equals("0.0"))
                    {lat ="3300000000";}
                else
                    {lat = String.valueOf(events.getDouble("lat"));}

                valid_position = String.valueOf(events.getBoolean("valid_position"));

                if(events.get("system_time").equals("null"))
                    {system_time ="";}
                else
                    {system_time = events.getString("system_time");}

                if(String.valueOf(events.get("speed")).equals("null"))
                    {speed ="0.0";}
                else
                    {speed = String.valueOf(events.getDouble("speed"));}

                if(String.valueOf(events.get("id")).equals("null"))
                    {id ="0.0";}
                else
                    {id = String.valueOf(events.getDouble("id"));}

                if(String.valueOf(events.get("device_id")).equals("null"))
                    {device_id ="0.0";}
                else
                    {device_id = String.valueOf(events.getDouble("device_id"));}

                if((!(Argumentos[x].equals("lat"))) && (!(Argumentos[x].equals("lon")))&& (!(Argumentos[x].equals("type"))) ){
                Datos= Datos +String.valueOf(events.get(Argumentos[x])+",");
                }

                if((Argumentos[x].equals("ecu_rpm"))||(Argumentos[x].equals("ecu_dicstance"))||(Argumentos[x].equals("ecu_speed"))){ 
                   if((!(String.valueOf(events.get(Argumentos[x]))).equals("0.0"))||(!(events.isNull(Argumentos[x])))) {

                         Enviar=true;
                        }     

                   }
           
            }
           System.out.println("|--------------------------------------------------|");
            System.out.println("     ID : "+ID+" Vehiculo :"+ name                   );
            System.out.println("|--------------------------------------------------|");
//            System.out.println("Head : "+head);
//            System.out.println("Code : "+code);
//            System.out.println("Hdop : "+hdop);
//            System.out.println("Event_time : "+event_time);
//            System.out.println("Type : "+type);
//            System.out.println("Vid : "+vid);
//            System.out.println("Lon : "+lon);
//            System.out.println("Sv : "+sv);
//            System.out.println("Mph : "+mph);
//            System.out.println("Label : "+label);
//            System.out.println("Valid_position : " +valid_position);
//            System.out.println("System_time : "+system_time);
//            System.out.println("Lat : "+lat);        
//            System.out.println("Speed : "+speed);
//            System.out.println("Id : "+id);
//            System.out.println("Device_id : "+device_id);
            
            
            String codeA[]  = code.split("\\.");
            String typeA[]  = type.split("\\.");
            String speedA[] = speed.split("\\.");
            String headA[]  = head.split("\\.");
            
            Tipo=Integer.parseInt(typeA[0]);
            Code=Integer.parseInt(codeA[0]);
            Lat = Long.parseLong(lat.replace(".",""));
            Lon = Long.parseLong(lon.replace(".",""));
            Veh=name;
            Fecha=event_time.substring(8,10)+event_time.substring(5,7)+event_time.substring(2,4).replace("-","");
            Hora =event_time.substring(11,19).replace(":","") ;
            Vel= Integer.parseInt(speedA[0]);
            Dir=Integer.parseInt(headA[0]);
            
            System.out.println("|---------------------------------|");
            System.out.println("              Ingreso :            ");
            System.out.println("|---------------------------------|");
            System.out.println("codigo : "+Code);
            System.out.println("Tipo : "+Tipo);
            System.out.println("Lat : "+Lat);
            System.out.println("Lon : "+Lon);
            System.out.println("Fecha : "+Fecha);
            System.out.println("Hora : "+Hora);
            System.out.println("Vel : "+Vel);
            System.out.println("Dir : "+Dir);
            
     
                 
            System.out.println("Datos : "+Datos.substring(0, Datos.length()-1).replace("null","0").replace("0.0","0"));
            
          
                if((valid_position.equals("false"))||(lat.replace(".","").equals("3300000000"))|| (lon.replace(".","").equals("7700000000")) || (Tipo==301))
                  {
                      Tipo=301;
                      insertReg(Tipo,Code,Lat,Lon,Veh,Fecha,Hora,Vel,Dir,conn);
                  }
              else
                {
                
                        insertReg(Tipo,Code,Lat,Lon,Veh,Fecha,Hora,Vel,Dir,conn);
                        
                        if(Enviar=true && Tipo!=10 )
                        {
                         insertRxc4(Tipo,Lat,Lon,ID,Veh,Fecha,Hora,Datos,conn);
                         Enviar=false;
                        }
                          
                    
                }                
                Datos=""; 
                Trama="";      
          
        }
    }
    } 
    
   public static void insertRxc4(int tp, long lat, long lon, int ID, String veh, String fch, String hr, String datos, Connection conn) throws Exception
    {
        System.out.println("|-----------------|"); 
        System.out.println("    INSERT RXC4    ");
        System.out.println("|-----------------|"); 
       LatStr = String.valueOf(lat);
        if (LatStr.length()<7)
            LatStr = LatStr.substring(1);
        else
            LatStr = LatStr.substring(1, 7);
        LatAux = LatStr.substring(0, 2) + "." + LatStr.substring(2);
        LonStr = String.valueOf(lon);
        
        if (LonStr.length()<7)
            LonStr = LonStr.substring(1);
        else
            LonStr = LonStr.substring(1, 7);
        LonAux = LonStr.substring(0, 2) + "." + LonStr.substring(2);
        
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
         rs = st.executeQuery("select CodEmpresa, id_veh from Vehiculo where Patente='"+veh+"'");
        String codEmpresa;
        rs.last();
            if (rs.getRow()>0)
            {
                codEmpresa=rs.getObject(1).toString();
                System.out.println("CodEmpresa : "+codEmpresa);
            }
            else
            {
                codEmpresa = "1";
                System.out.println("CodEmpresa : "+codEmpresa);
            }
         
        
        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = st.executeQuery("select DATEDIFF(hour,GETUTCDATE(),GETDATE())");
        rs.first();
        ZonaHoraria = rs.getObject(1).toString();
        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = st.executeQuery("select top 1 isnull(CodLugar,0) from Lugar where abs(Lat-"+LatAux+")<=radio/convert(float,120000) and abs(Lon-"+LonAux+")<=radio/convert(float,90000) and CodEmpresa="+codEmpresa);

        rs.last();
            if (rs.getRow()>0)
            {
                rs.first();
                CodLugar = rs.getObject(1).toString();
            }
            else
            {
                CodLugar = "1";
            }
        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = st.executeQuery("select id_veh from Vehiculo where Patente='"+veh+"'");
        rs.last();
        if (rs.getRow()>0)
        {
            rs.first();
            id_veh = rs.getObject(1).toString();
            st = conn.createStatement();
            rs1 = st.executeQuery("select dateadd(hh,"+ZonaHoraria+",convert(datetime,'20"+fch.substring(4)+"-"+fch.substring(2,4)+"-"+fch.substring(0,2)+" "+hr.substring(0,2)+":"+hr.substring(2,4)+":"+hr.substring(4)+"',102))");
            rs1.next();
            FechaRx = rs1.getObject(1).toString();
            fch = fch.substring(0,2)+fch.substring(2,4)+fch.substring(4);
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs2 = st.executeQuery("select TOP 1 * from rxc4 where imei='111111111111111' and data like '%"+veh+"%' order by id desc");
            rs2.last();
            if (rs2.getRow()>0)
                {
                    rs2.first();
                    FechaAux = rs2.getObject(5).toString();  
                }
                else
                {
                    registros=false;
                    rest=true;
                }
            System.out.println("Hay fecha = " + registros +"\nFecha de ultimo registro = " + FechaAux +"\n"+"Fecha a ingresar = " + FechaRx +"\n" );
            
            if(!FechaAux.equals("")){
            String ultimoReg=FechaAux.substring(0, FechaAux.length()-2);
            String aInsertar=FechaRx.substring(0, FechaRx.length()-2);
           
            System.out.println("FORMATO ultimoReg" + ultimoReg +"\n FORMATO aInsertar= " + aInsertar +"\n" );
            
            
            rest= compararFechasConDate(ultimoReg,aInsertar);
            }
        
        if ((rest==true)||(registros==false)) 
            {
                System.out.println("insert into rxc4 values ('111111111111111','0','"+tp+","+ID+","+veh+","+fch+","+hr+datos.substring(0, datos.length()-1).replace("null","0").replace("0.0","0")+LatStr+","+LonStr+"','"+FechaRx+"','17','0')");
                st = conn.createStatement();
                st.executeUpdate("insert into rxc4 values ('111111111111111','0','"+tp+","+ID+","+veh+","+fch+","+hr+datos.substring(0, datos.length()-1).replace("null","0").replace("0.0","0")+LatStr+","+LonStr+"','"+FechaRx+"','17','0')");
                
                
                 Tramas="insert into rxc4 values ('111111111111111','0','"+tp+","+ID+","+veh+","+fch+","+hr+datos.substring(0, datos.length()-1).replace("null","0").replace("0.0","0")+LatStr+","+LonStr+"','"+FechaRx+"','17','0')";
                 Tramas=Tramas.toString()+"\n";
                 System.out.println("REG: "+Tramas+"");
                 Log.log("REG obtenido: "+Tramas.substring(0, Tramas.length()-1));
              
                
                FechaAux="";
                registros=true;
                 rest=false;
                Datos="";
            }
        }
        else
        {
            st = conn.createStatement();
            rs1 = st.executeQuery("select dateadd(hh,"+ZonaHoraria+",convert(datetime,'20"+fch.substring(4)+"-"+fch.substring(2,4)+"-"+fch.substring(0,2)+" "+hr.substring(0,2)+":"+hr.substring(2,4)+":"+hr.substring(4)+"',102))");
            rs1.next();
            FechaRx = rs1.getObject(1).toString();
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs1 = st.executeQuery("select top 1 id_veh from Vehiculo order by id_veh desc");
            rs1.first();
            id_veh = rs1.getObject(1).toString();
            BigInteger bi1 = new BigInteger(id_veh);
            BigInteger bi2 = new BigInteger("1");
            BigInteger bir = bi1.add(bi2);
            id_veh = bir.toString();
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            st.executeUpdate("insert into Vehiculo values ('"+id_veh+"','"+Veh+"',1)");
            st = conn.createStatement();
	    st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	    st.executeUpdate("insert into FlotaVehiculo values (1,'"+id_veh+"')");
                System.out.println("insert into rxc4 values ('111111111111111','0','"+tp+","+ID+","+veh+","+fch+","+hr+datos.substring(0, datos.length()-1).replace("null","0").replace("0.0","0")+LatStr+","+LonStr+"','"+FechaRx+"','17','0')");
            st = conn.createStatement();
            st.executeUpdate("insert into rxc4 values ('111111111111111','0','"+tp+","+ID+","+veh+","+fch+","+hr+datos.substring(0, datos.length()-1).replace("null","0").replace("0.0","0")+LatStr+","+LonStr+"','"+FechaRx+"','17','0')");
            registros=true;
             rest=false;
             Datos="";
          }
        rs.close();
    }
    
     
public static void insertReg(int tp,int cod, long lat, long lon, String veh, String fch, String hr, int vel, int dir, Connection conn) throws Exception
    {
        System.out.println("|-----------------|"); 
        System.out.println("    INSERT REG    ");
        System.out.println("|-----------------|"); 
        
         if(tp==301) 
       { 
            switch(tp)
           {
               case 301:
                   codreg = "1";
                   break;        
           }
       }
    else{
         
       if(cod!=2 && cod!=3 && cod!=4 ) 
       {
        codreg = "25";
       }
       else{
           
           switch(cod)
                {
                    case 2:
                         codreg = "22";
                         System.out.println("2|---------22--------|"); 
                         break;
                    case 3:
                         codreg = "23";
                         System.out.println("3|---------23--------|"); 
                         break;
                    case 4:
                         codreg = "5";
                         System.out.println("4|---------5--------|"); 
                         break;
                }
            }
       
        }
        LatStr = String.valueOf(lat);
        if (LatStr.length()<7)
            LatStr = LatStr.substring(1);
        else
            LatStr = LatStr.substring(1, 7);
        LatAux = LatStr.substring(0, 2) + "." + LatStr.substring(2);
        LonStr = String.valueOf(lon);
        if (LonStr.length()<7)
            LonStr = LonStr.substring(1);
        else
            LonStr = LonStr.substring(1, 7);
        LonAux = LonStr.substring(0, 2) + "." + LonStr.substring(2);
         st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
         rs = st.executeQuery("select CodEmpresa, id_veh from Vehiculo where Patente='"+veh+"'");
                
         String codEmpresa;
        rs.last();
            if (rs.getRow()>0)
            {
                codEmpresa=rs.getObject(1).toString();
                System.out.println("CodEmpresa : "+codEmpresa);
            }
            else
            {
                codEmpresa = "1";
                System.out.println("CodEmpresa : "+codEmpresa);
            }
        
        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = st.executeQuery("select DATEDIFF(hour,GETUTCDATE(),GETDATE())");
        rs.first();
        ZonaHoraria = rs.getObject(1).toString();
        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = st.executeQuery("select top 1 isnull(CodLugar,0) from Lugar where abs(Lat-"+LatAux+")<=radio/convert(float,120000) and abs(Lon-"+LonAux+")<=radio/convert(float,90000) and CodEmpresa="+codEmpresa);
        //System.out.println("select top 1 isnull(CodLugar,0) from Lugar where abs(Lat-"+LatAux+")<=radio/convert(float,120000) and abs(Lon-"+LonAux+")<=radio/convert(float,90000) and CodEmpresa="+codEmpresa);
        rs.last();
        if (rs.getRow()>0)
            {
                rs.first();
                CodLugar = rs.getObject(1).toString();
            }
        else
            {
                CodLugar = "1";
            }
        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = st.executeQuery("select id_veh from Vehiculo where Patente='"+veh+"'");
        rs.last();
        if (rs.getRow()>0)
        {
            rs.first();
            id_veh = rs.getObject(1).toString();
            st = conn.createStatement();
            rs1 = st.executeQuery("select dateadd(hh,"+ZonaHoraria+",convert(datetime,'20"+fch.substring(4)+"-"+fch.substring(2,4)+"-"+fch.substring(0,2)+" "+hr.substring(0,2)+":"+hr.substring(2,4)+":"+hr.substring(4)+"',102))");
            rs1.next();
            FechaRx = rs1.getObject(1).toString();
            fch = "20"+fch.substring(4)+fch.substring(2,4)+fch.substring(0,2);
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs2 = st.executeQuery("select TOP 1 * from RegRx where id_veh='"+id_veh+"' order by CodRegRx desc");
            rs2.last();
            if (rs2.getRow()>0)
                {
                    rs2.first();
                    FechaAux = rs2.getObject(9).toString();  
                }
            else
                {
                    registros=false;
                    rest=true;
                }
        System.out.println("Hay fecha = " + registros +"\nFecha de ultimo registro = " + FechaAux +"\n"+"Fecha a ingresar = " + FechaRx +"\n" );
        
           if(!FechaAux.equals("")){
            String ultimoReg=FechaAux.substring(0, FechaAux.length()-2);
            String aInsertar=FechaRx.substring(0, FechaRx.length()-2);
             System.out.println("FORMATO ultimoReg" + ultimoReg +"\n FORMATO aInsertar= " + aInsertar +"\n" );
            
            rest= compararFechasConDate(ultimoReg,aInsertar);
            }
          
        if (((rest==true))||(registros==false)) 
            {
                if(codreg.equals("22")){
                st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rs = st.executeQuery("select top 1 Fecha_Hora from DiaRegRx where id_veh='"+id_veh+"' and CodReg='23'order by CodRegRx desc");   
                rs.last();
                       if (rs.getRow()>0)
                        {
                        String ultimo23;
                        ultimo23=rs.getObject(1).toString();
                        System.out.println("Ultima IgnOFF,'"+ultimo23+"')");
                        System.out.println("select datediff (MINUTE,'"+ultimo23+"','"+FechaRx+"')");
                        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                        rs = st.executeQuery("select datediff (MINUTE,'"+ultimo23+"','"+FechaRx+"')");
                        rs.last();
                            if (rs.getRow()>0)
                               {
                               String minutos;
                               minutos=rs.getObject(1).toString();
                               vel=Integer.parseInt(minutos);
                               System.out.println("(Ev 22)MIN desde la ultima IgnOFF :"+vel);
                               }
                        }
                    else
                        {
                            vel=0;
                        }
               
                }else {
					
		if(codreg.equals("23")){
                    st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    rs = st.executeQuery("select top 1 Fecha_Hora from DiaRegRx where id_veh='"+id_veh+"' and CodReg='22'order by CodRegRx desc");   
                    rs.last();
                    if (rs.getRow()>0)
                        {
                        String ultimo22;
                        ultimo22=rs.getObject(1).toString();
                        System.out.println("Ultima IgnON,'"+ultimo22+"')");
                        System.out.println("select datediff (MINUTE,'"+ultimo22+"','"+FechaRx+"')");
                        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                        rs = st.executeQuery("select datediff (MINUTE,'"+ultimo22+"','"+FechaRx+"')");
                        rs.last();
                            if (rs.getRow()>0)
                            {
                            String minutos;
                            minutos=rs.getObject(1).toString();
                            vel=Integer.parseInt(minutos);
                            System.out.println("(Ev 23) MIN desde la ultima IgnON :"+vel);
                            }
                        }
                    else
                        {
                            vel=0;
                        }
                    }
		}
                
                    System.out.println("insert into DiaRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','          ','"+vel+"','"+dir+"')");
                st = conn.createStatement();
                st.executeUpdate("insert into DiaRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','          ','"+vel+"','"+dir+"')");
                    System.out.println("insert into DiasRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','          ','"+vel+"','"+dir+"')");
                st = conn.createStatement();
                st.executeUpdate("insert into DiasRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','          ','"+vel+"','"+dir+"')");
                    System.out.println("insert into RegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','          ','"+vel+"','"+dir+"')");
                st = conn.createStatement();
                st.executeUpdate("insert into RegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','          ','"+vel+"','"+dir+"')");
                st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rs1 = st.executeQuery("select codregrx from lastregrx where id_veh='"+id_veh+"'");
                rs1.last();
                
                Tramas="insert into RegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','          ','"+vel+"','"+dir+"')";
                Tramas=Tramas.toString()+"\n";
                System.out.println("REG: "+Tramas+"");
                Log.log("REG obtenido: "+Tramas.substring(0, Tramas.length()-1));
                
                FechaAux="";
                registros=true;
                rest=false;
                if (rs1.getRow()>0)
                {
                    st = conn.createStatement();
                    st.executeUpdate("update LastRegRx set CodReg="+codreg+",CodLugar="+CodLugar+",CodFecha="+fch+",CodSubTipoReg=0,CodSubTipoTaco=0,CodSec=0,Fecha_hora=convert(datetime,'"+FechaRx+"',102),Lugar='"+LatStr+LonStr+"',RutConductor='',Lat='"+LatStr+"',Lon='"+LonStr+"',Generico1='          ',Generico2='"+vel+"',Generico3='"+dir+"' where id_veh='"+id_veh+"'");
                }
                else
                {
                    st = conn.createStatement();
                    st.executeUpdate("insert into LastRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','          ','"+vel+"','"+dir+"')");
                }
                
                
            }
        }
        else
        {
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs1 = st.executeQuery("select top 1 id_veh from Vehiculo order by id_veh desc");
            rs1.first();
            id_veh = rs1.getObject(1).toString();
            BigInteger bi1 = new BigInteger(id_veh);
            BigInteger bi2 = new BigInteger("1");
            BigInteger bir = bi1.add(bi2);
            id_veh = bir.toString();
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            st.executeUpdate("insert into Vehiculo values ('"+id_veh+"','"+Veh+"',1)");
            st = conn.createStatement();
	    st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	    st.executeUpdate("insert into FlotaVehiculo values (1,'"+id_veh+"')");
   
	    st = conn.createStatement();
            rs1 = st.executeQuery("select dateadd(hh,"+ZonaHoraria+",convert(datetime,'20"+fch.substring(4)+"-"+fch.substring(2,4)+"-"+fch.substring(0,2)+" "+hr.substring(0,2)+":"+hr.substring(2,4)+":"+hr.substring(4)+"',102))");
            rs1.next();
            FechaRx = rs1.getObject(1).toString();
            fch = "20"+fch.substring(4)+fch.substring(2,4)+fch.substring(0,2);
                System.out.println("insert into DiaRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','','"+vel+"','"+dir+"')");
            st = conn.createStatement();
            st.executeUpdate("insert into DiaRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','','"+vel+"','"+dir+"')");
                System.out.println("insert into DiasRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','','"+vel+"','"+dir+"')");
            st = conn.createStatement();
            st.executeUpdate("insert into DiasRegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','','"+vel+"','"+dir+"')");
         
                System.out.println("insert into RegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','','"+vel+"','"+dir+"')");
            st = conn.createStatement();
            st.executeUpdate("insert into RegRx values ("+codreg+","+CodLugar+",'"+fch+"','"+id_veh+"',0,0,0,convert(datetime,'"+FechaRx+"',102),'"+LatStr+LonStr+"','','"+LatStr+"','"+LonStr+"','','"+vel+"','"+dir+"')");
        }
        rs.close();
    }


static boolean compararFechasConDate(String ultimoReg, String aInsertar) {  
  boolean resultado=false;
  try {
   /**Obtenemos las fechas enviadas en el formato a comparar*/
   SimpleDateFormat formateador = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
   Date fechaDate1 = formateador.parse(ultimoReg);
   Date fechaDate2 = formateador.parse(aInsertar);

    if ( fechaDate1.before(fechaDate2) ){
        resultado=true;

    }else{
     if ( fechaDate2.before(fechaDate1) ){
        resultado=false;

     }else{
        resultado=false;
     } 
    }
  } catch (ParseException e) {
   System.out.println("Se Produjo un Error!!!  "+e.getMessage());
  }  
  return resultado;
 
    }

}