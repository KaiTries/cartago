/**
 * CArtAgO - DISI, University of Bologna
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package cartago.infrastructure.web;

import java.rmi.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import cartago.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;


/**
 * Class representing a CArtAgO node service, serving remote requests
 *  
 * @author aricci
 *
 */
public class CartagoEnvironmentService extends AbstractVerticle  {

	private String fullAddress;
	private int port;
		
	private Router router;
	private HttpServer server;
	
	private String envName;
	
	private Logger logger = LoggerFactory.getLogger(CartagoEnvironmentService.class);
	
	private static final String API_BASE_PATH = "/cartago/api";

	private ConcurrentLinkedQueue<AgentBodyRemote> remoteCtxs;
	// private GarbageBodyCollectorAgent garbageCollector;
	private ConcurrentHashMap<String, AgentBody> pendingBodies;
	
	
	public CartagoEnvironmentService() throws Exception {
		remoteCtxs = new ConcurrentLinkedQueue<AgentBodyRemote>();	
		// garbageCollector = new GarbageBodyCollectorAgent(remoteCtxs,1000,10000);
		// garbageCollector.start();
		pendingBodies = new  ConcurrentHashMap<String, AgentBody>();
	}	
		
	public void install(String address, int port) throws Exception {
		/* WARNING: the  timeout - 1000 - must be greater than the 
		   delay used by the KeepRemoteContextAliveManager
		   to keep alive the remote contexts */
		//
		this.port = port;
		fullAddress = address+":"+port;

		Vertx vertx = Vertx.vertx();
		vertx.deployVerticle(this);
	}	

	
	@Override
	public void start() {
		initWS();	
	}


	private void initWS() {
		this.router = Router.router(this.vertx);
		this.router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST).allowedMethod(HttpMethod.PUT).allowedMethod(HttpMethod.DELETE).allowedMethod(HttpMethod.OPTIONS).allowedHeader("Access-Control-Request-Method").allowedHeader("Access-Control-Allow-Credentials").allowedHeader("Access-Control-Allow-Origin").allowedHeader("Access-Control-Allow-Headers").allowedHeader("Content-Type"));
		this.router.route().handler(BodyHandler.create());
		this.router.get("/cartago/api/version").handler(this::handleGetVersion);
		this.router.get("/cartago/api/:masName").handler(this::handleResolveWSP);
		this.server = this.vertx.createHttpServer().requestHandler(this.router).webSocketHandler((ws) -> {
			if (ws.path().equals("/cartago/api/join")) {
				this.handleJoinWSP(ws);
			} else {
				ws.reject();
			}

		}).listen(this.port, (result) -> {
			if (result.succeeded()) {
				this.log("Ready.");
			} else {
				this.log("Failed: " + result.cause());
			}

		});
	}

	private void log(String msg) {
		System.out.println("[ CArtAgO Web Service Layer ] " + msg);
	}

	public void shutdownService() {
		this.server.close();
	}

	public void registerLogger(String wspName, ICartagoLogger logger) throws RemoteException, CartagoException {
		CartagoEnvironment.getInstance().registerLogger(wspName, logger);
	}

	private void handleJoinWSP(ServerWebSocket ws) {
		SocketAddress var10001 = ws.remoteAddress();
		this.log("Handling Join WSP from " + var10001 + " - " + ws.path());
		CartagoEnvironmentService service = this;
		ws.handler((buffer) -> {
			JsonObject joinParams = buffer.toJsonObject();
			String wspName = joinParams.getString("wspFullName");
			JsonObject agentCred = joinParams.getJsonObject("agent-cred");
			String userName = agentCred.getString("userName");
			String roleName = agentCred.getString("roleName");
			AgentCredential cred = new AgentIdCredential(userName, roleName);

			try {
				Workspace wsp = CartagoEnvironment.getInstance().resolveWSP(wspName).getWorkspace();
				System.out.println("Remote request to join: " + wspName + " " + roleName + " " + cred);
				AgentBodyRemote rbody = new AgentBodyRemote();
				ICartagoContext ctx = wsp.joinWorkspace(cred, rbody);
				this.remoteCtxs.add(rbody);
				rbody.init((AgentBody)ctx, ws, service);
				JsonObject reply = (new JsonObject()).put("wspUUID", wsp.getId().getUUID().toString());
				ws.writeTextMessage(reply.encode());
			} catch (Exception var14) {
				Exception ex = var14;
				ex.printStackTrace();
				ws.reject();
			}

		});
	}

	public void registerNewJoin(String bodyId, AgentBody body) {
		this.pendingBodies.put(bodyId, body);
	}

	private void handleQuitWSP(RoutingContext routingContext) {
		this.log("Handling Quit WSP from " + routingContext.request().absoluteURI());
		HttpServerResponse response = routingContext.response();
		response.putHeader("content-type", "application/text").end("Not implemented.");
	}

	private void handleExecIAOP(RoutingContext routingContext) {
		this.log("Handling Exec Inter artifact OP from " + routingContext.request().absoluteURI());
		HttpServerResponse response = routingContext.response();
		response.putHeader("content-type", "application/text").end("Not implemented.");
	}

	private void handleGetVersion(RoutingContext routingContext) {
		this.log("Handling Get Version from " + routingContext.request().absoluteURI());
		HttpServerResponse response = routingContext.response();
		response.putHeader("content-type", "application/text").end(CARTAGO_VERSION.getID());
	}

	private void handleResolveWSP(RoutingContext routingContext) {
		this.log("Handling ResolveWSP from " + routingContext.request().absoluteURI());
		String envName = routingContext.request().getParam("masName");
		String fullPath = routingContext.request().getParam("wsp");
		JsonObject obj = new JsonObject();

		try {
			WorkspaceDescriptor des = CartagoEnvironment.getInstance().resolveWSP(fullPath);
			obj.put("envName", des.getEnvName());
			obj.put("envId", des.getEnvId().toString());
			if (des.isLocal()) {
				obj.put("id", JsonUtil.toJson(des.getId()));
			} else {
				obj.put("remotePath", des.getRemotePath());
				obj.put("address", des.getAddress());
				obj.put("protocol", des.getProtocol());
			}

			routingContext.response().putHeader("content-type", "application/text").end(obj.encode());
		} catch (Exception var7) {
			HttpServerResponse response = routingContext.response();
			response.setStatusCode(404).end();
		}

	}
}
