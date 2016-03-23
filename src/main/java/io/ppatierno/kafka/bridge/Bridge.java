package io.ppatierno.kafka.bridge;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;
import io.vertx.proton.ProtonSession;

/**
 * Main bridge class listening for connections
 * and handling AMQP senders and receivers
 * 
 * @author ppatierno
 */
public class Bridge {

	private static final int DEFAULT_PORT = 5672;
	private static final String DEFAULT_HOST = "localhost";
	
	private static final Logger LOG = LoggerFactory.getLogger(Bridge.class);
	
	private Vertx vertx;
	private ProtonServer server;
	private int port;
	private String host;
	
	private BridgeEndpoint producer;
	private BridgeEndpoint consumer;
	
	
	// TODO : just for testing, another class for launching bridge ?
	public static void main(String[] args) {
		
		String config = (args.length > 0 && !args[0].isEmpty()) ? args[0] : null;
		
		Vertx vertx = Vertx.vertx();
		
		Bridge bridge = new Bridge(vertx, config);
		bridge.start();
		
		try {
			System.in.read();
			bridge.stop();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Constructor
	 * @param vertx		Vertx instance used to run the Proton server
	 * @param config	configuration file path
	 * @throws Exception 
	 */
	public Bridge(Vertx vertx, String config) {
		
		if (vertx == null) {
			throw new NullPointerException("Vertx instance cannot be null");
		}
		
		// if provided as parameter, load configuration from file otherwise the default one
		if (config != null && !config.isEmpty()) {
			if (!BridgeConfig.load(config))
				throw new IllegalArgumentException("Configuration file path is not valid");
		} else {
			BridgeConfig.loadDefault();
		}
		
		this.vertx = vertx;
		
		this.host = DEFAULT_HOST;
		this.port = DEFAULT_PORT;
		
		this.producer = new InputBridgeEndpoint();
		this.consumer = new OutputBridgeEndpoint();
	}
	
	/**
	 * Start the bridge
	 */
	public void start() {
		
		ProtonServerOptions options = this.createServerOptions();
		
		this.server = ProtonServer.create(this.vertx, options)
				.connectHandler(this::processConnection)
				.listen(ar -> {
					
					if (ar.succeeded()) {
						LOG.info("AMQP-Kafka Bridge is listening on port {}", ar.result().actualPort());
						
						this.producer.open();
						this.consumer.open();
						
					} else {
						LOG.error("Error starting AMQP-Kafka Bridge", ar.cause());
					}
				});
		
	}
	
	/**
	 * Stop the bridge
	 */
	public void stop() {
		
		if (this.server != null) {
			this.server.close();
			
			this.producer.close();
			this.consumer.close();
		}
	}
	
	/**
	 * Create an options instance for the ProtonServer
	 * based on AMQP-Kafka bridge internal configuration
	 * 
	 * @return		ProtonServer options instance
	 */
	private ProtonServerOptions createServerOptions(){
		
		ProtonServerOptions options = new ProtonServerOptions();
		options.setHost(this.host);
		options.setPort(this.port);
		return options;
	}
	
	/**
	 * Process a connection request accepted by the Proton server
	 * 
	 * @param connection		Proton connection accepted instance
	 */
	private void processConnection(ProtonConnection connection) {
		
		connection
		.openHandler(this::processOpenConnection)
		.closeHandler(this::processCloseConnection)
		.disconnectHandler(this::processDisconnection)
		.sessionOpenHandler(this::processOpenSession)
		.receiverOpenHandler(this::processOpenReceiver)
		.senderOpenHandler(this::processOpenSender)
		.open();
	}
	
	/**
	 * Handler for connection opened by remote
	 * @param ar		async result with info on related Proton connection
	 */
	private void processOpenConnection(AsyncResult<ProtonConnection> ar) {
		if (ar.succeeded()) {
			LOG.info("Connection opened by {} {}", ar.result().getRemoteHostname(), ar.result().getRemoteContainer());
		}
	}
	
	/**
	 * Handler for connection closed by remote
	 * @param ar		async result with info on related Proton connection
	 */
	private void processCloseConnection(AsyncResult<ProtonConnection> ar) {
		if (ar.succeeded()) {
			LOG.info("Connection closed by {} {}", ar.result().getRemoteHostname(), ar.result().getRemoteContainer());
			ar.result().close();
		}
	}
	
	/**
	 * Handler for disconnection from the remote
	 * @param connection	related Proton connection closed
	 */
	private void processDisconnection(ProtonConnection connection) {
		LOG.info("Disconnection from {} {}", connection.getRemoteHostname(), connection.getRemoteContainer());
	}
	
	/**
	 * Handler for session opened by remote
	 * @param session		related Proton session object
	 */
	private void processOpenSession(ProtonSession session) {
		LOG.info("Session opened");
		session.open();
	}
	
	/**
	 * Handler for attached link by a remote sender
	 * @param receiver		receiver link created by the underlying Proton library
	 * 						by which handling communication with remote sender
	 */
	private void processOpenReceiver(ProtonReceiver receiver) {
		LOG.info("Remote sender attached");
		
		receiver.setAutoAccept(false);
		// TODO : one or producers pool ?
		this.producer.handle(receiver);
	}
	
	/**
	 * Handler for attached link by a remote receiver
	 * @param sender		sende link created by the underlying Proton library
	 * 						by which handling communication with remote receiver
	 */
	private void processOpenSender(ProtonSender sender) {
		LOG.info("Remote receiver attached");
		
		// TODO : one or consumers pool ?
		this.consumer.handle(sender);
	}
}