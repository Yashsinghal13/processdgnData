package com.octro.utilities

import java.util.Properties
import javax.mail.Session
import javax.mail.Transport
import javax.mail.MessagingException
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
import javax.mail.Message
import java.util.Date
import javax.mail.Address

object Notifier {
  def main(args:Array[String]){
    Notifier.sendEMail("from-no-reply@octro.com", "test email body", "ashutosh.sharma@octro.com,sukrit.mehta@octro.com", "test subject")
  }
	def sendEMail (senderEMail:String, eMailBody:String, receiverEmailsCSV:String, eMailSubject:String)={
			if ((receiverEmailsCSV == null) && (receiverEmailsCSV.equals(""))){
				OPrinter.logAndPrint("Email ID not defined")
			}
			val smtpHost:String = "serv126"
			val prop:Properties = new Properties()
			prop.put("mail.smtp.host", smtpHost)
			prop.put("mail.debug", "false")
			var session:Session = Session.getInstance(prop)
			var toPersonList:Array[String] = receiverEmailsCSV.split("\\,")
			var toMailListSB:StringBuffer = new StringBuffer()
			var toPersonName:String = ""
			var toMailId:String = ""
			var index:Int = 0
			if (toPersonList.size > 0) {
			  for(index <- 0 to toPersonList.length-1){
  			  println("index = " +  index)
  				toMailId = toPersonList(index).asInstanceOf[String]
					toMailListSB.append(toMailId)
					toMailListSB.append(",")                 //toMailListSB.append(";")
  			}
			}else{                                       //When mailIdList has just one email ID, with no comma
			  toMailListSB.append(receiverEmailsCSV)
			  toMailListSB.append(",")
			}
			try{
				var msg:MimeMessage = new MimeMessage(session)
						msg.setFrom(new InternetAddress(senderEMail))
						var toList:Array[String] = toMailListSB.toString().split(",")
						var address = new Array[Address](toList.length)
								var i:Int = 0
								for(i <- 0 to toList.length-1){
									address(i) = new InternetAddress(toList(i)).asInstanceOf[Address]
								}
						msg.setRecipients(Message.RecipientType.TO, address)
						msg.setHeader("Content-Type", "text/html")
						msg.setSubject(eMailSubject)
						msg.setSentDate(new Date())       
						msg.setContent(eMailBody, "text/html")
						Transport.send(msg)
						OPrinter.logAndPrint("Email successfully sent with below payload.\n\nSender = " + senderEMail + "\nReceivers = " + receiverEmailsCSV + "\nEmail Subject = " + eMailSubject + "\nEmail Body = " + eMailBody + "\n")
			}
			catch{
			case me:MessagingException =>{
				me.printStackTrace()
				OPrinter.logAndPrint("<---Error in method sending mail ---> " + me)
			}
			} }

}
