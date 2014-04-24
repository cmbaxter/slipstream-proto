package com.aquto.slipstream.couch

import javax.security.auth.callback._

/**
 * Callback handler for doing plain auth.
 */
class PlainCallbackHandler(username:String, password:Array[Char]) extends CallbackHandler {

  def handle(callbacks:Array[Callback]) {
    
    callbacks foreach{ cb =>
      cb match{
          
        case ncb:NameCallback =>
          ncb.setName(username)
          
        case pcb:PasswordCallback =>
          pcb.setPassword(password)
          
        case _ =>
          throw new UnsupportedCallbackException(cb)
      }
    }
  }
}