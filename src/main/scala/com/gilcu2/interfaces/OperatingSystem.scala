package com.gilcu2.interfaces

object OperatingSystem {

  def getHostname: String = java.net.InetAddress.getLocalHost.getHostName

}
