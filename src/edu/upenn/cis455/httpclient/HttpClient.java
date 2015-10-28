package edu.upenn.cis455.httpclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import java.text.ParseException;


public class HttpClient {
	HashMap<String, String> responseData;
	String hostName;
	int portNum;
	String queryString;
	
	public HttpClient(String urlAndPort, String queryString) {
		responseData = new HashMap<String, String>();
		hostName = urlAndPort.substring(0, urlAndPort.indexOf(':'));
		portNum = Integer.parseInt(urlAndPort.substring(urlAndPort.indexOf(':') + 1));
		this.queryString = queryString;
	}
	
	public void makeRequest() throws IOException {
		InetAddress ip = InetAddress.getByName(hostName);
		Socket socket = new Socket(ip, portNum);
		String request = getHeaders();
		byte[] response = request.getBytes("UTF-8");
		OutputStream outStream = socket.getOutputStream();
		outStream.write(response,0,response.length);
		outStream.flush();
		readResponse(socket.getInputStream());
		socket.close();
	}
	
	protected void readResponse(InputStream inStream) throws IOException {
		StringBuilder response = new StringBuilder();
		BufferedReader in = new BufferedReader(new InputStreamReader(inStream));
		String line;
		boolean pastBreak = false;
		while ((line = in.readLine()) != null) {
			if (line.startsWith("HTTP/1.")){
				String[] pieces = line.split(" ");
				if (pieces.length < 3)
					throw new IllegalStateException("HTTP header line had improper format.");
				responseData.put("Protocol", pieces[0]);
				responseData.put("ResponseCode", pieces[1]);
				StringBuilder message = new StringBuilder();
				for (int i = 2; i < pieces.length; i++) {
					message.append(pieces[i]);
				}
				responseData.put("ResponseMessage", message.toString());
			}
			else if (line.equals(""))
					pastBreak = true;
			else if (!pastBreak) {
				responseData.put(line.substring(0, line.indexOf(":")).trim(), line.substring(line.indexOf(":") + 1).trim());
			}
			else {
				response.append(line + "\n");
			}
		}
		responseData.put("Body", response.toString());
		in.close();
	}
	
	protected String getHeaders() {
		StringBuilder headerString = new StringBuilder();
		headerString.append("GET /master/workerstatus" + queryString + " HTTP/1.0\r\n");
		headerString.append("Host: " + hostName + "\r\n");
		headerString.append("\r\n");
		return headerString.toString();
	}

	public String getResponseHeader(String name) {
		return responseData.get(name);
	}
	
	public String getBody() {
		return responseData.get("Body");
	}
}
