package com.octro.utilities

import org.apache.log4j.LogManager
import org.joda.time.DateTime

object OPrinter {

		def logAndPrint(msg: String) {
			val msgWithTStamp = DateTime.now().toString(Format.fmt) + " : " + msg
					println(msgWithTStamp)
					LogManager.getRootLogger.debug(msgWithTStamp)
		}

}