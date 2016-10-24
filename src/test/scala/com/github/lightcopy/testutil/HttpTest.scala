/*
 * Copyright 2016 Lightcopy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.lightcopy.testutil

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler.AbstractHandler

/** Http test server */
trait HttpTest {
  def makeRequest(
      reqHandler: (HttpServletRequest, HttpServletResponse) => Unit
    )(requestF: String => Unit): Unit = {

    val server = new Server(0)
    server.setHandler(new AbstractHandler() {
      def handle(
          target: String,
          baseRequest: Request,
          request: HttpServletRequest,
          response: HttpServletResponse): Unit = {
        reqHandler(request, response)
        baseRequest.setHandled(true)
      }
    })

    try {
      server.start()
      val port = server.getConnectors.head.getLocalPort
      requestF("http://localhost:" + port + "/")
    } finally {
      server.stop()
    }
  }
}
