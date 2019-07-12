package com.gilcu2.interfaces

object OS {

  def getHostname: String = java.net.InetAddress.getLocalHost.getHostName

}
