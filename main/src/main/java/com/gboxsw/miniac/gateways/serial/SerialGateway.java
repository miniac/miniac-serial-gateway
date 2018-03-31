package com.gboxsw.miniac.gateways.serial;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import jssc.*;

import com.gboxsw.miniac.*;
import com.gboxsw.miniac.utils.MonotonicClock;

/**
 * Gateway implementing messages based communication over a serial line.
 * 
 * Message is a sequence of bytes delimited by a delimiting character (byte).
 * Default delimiting character (byte) is the newline character.
 * 
 * The serial line is configured to use 8 databits, 1 stop bit and no parity
 * bit.
 */
public class SerialGateway extends Gateway {

	/**
	 * Configuration of gateway.
	 */
	public static class Configuration {
		/**
		 * Name of serial port.
		 */
		private String portName;

		/**
		 * Baud rate of serial line.
		 */
		private int baudRate;

		/**
		 * Code of character (byte) delimiting messages.
		 */
		private int messageDelimiter = '\n';

		/**
		 * Delay in milliseconds before restarting the connection after
		 * connection over the serial line failed.
		 */
		private long restartDelay = 2000;

		/**
		 * Initial delay before sending of messages start.
		 */
		private long sendingDelay = 1000;

		/**
		 * Maximal number of unsent messages to keep when serial line is broken.
		 */
		private int offlineBufferCapacity = 0;

		/**
		 * Constructs default configuration.
		 * 
		 * @param portName
		 *            the port name of serial port.
		 * @param baudRate
		 *            the baud rate of serial line.
		 */
		public Configuration(String portName, int baudRate) {
			this.portName = portName;
			setBaudRate(baudRate);
		}

		/**
		 * Returns the name of serial port.
		 * 
		 * @return the name of serial port.
		 */
		public String getPortName() {
			return portName;
		}

		/**
		 * Sets the name of serial port.
		 * 
		 * @param portName
		 *            the name of serial port.
		 */
		public void setPortName(String portName) {
			this.portName = portName;
		}

		/**
		 * Returns the baud rate of serial line.
		 * 
		 * @return the baud rate of serial line.
		 */
		public int getBaudRate() {
			return baudRate;
		}

		/**
		 * Sets the baud rate of serial line.
		 * 
		 * @param baudRate
		 *            the baud rate of serial line.
		 */
		public void setBaudRate(int baudRate) {
			if (baudRate <= 0) {
				throw new IllegalArgumentException("Baud rate must be non-zero positive integer.");
			}
			this.baudRate = baudRate;
		}

		/**
		 * Returns byte used as message delimiter.
		 * 
		 * @return the message delimiter in the range 0-255.
		 */
		public int getMessageDelimiter() {
			return messageDelimiter;
		}

		/**
		 * Sets byte used as message delimiter.
		 * 
		 * @param messageDelimiter
		 *            the message delimiter in the range 0-255.
		 */
		public void setMessageDelimiter(int messageDelimiter) {
			if ((messageDelimiter < 0) || (messageDelimiter > 255)) {
				throw new IllegalArgumentException("Message delimiter must be in range 0-255.");
			}

			this.messageDelimiter = messageDelimiter;
		}

		/**
		 * Returns the delay in milliseconds before restarting the
		 * communication.
		 * 
		 * @return the delay in milliseconds.
		 */
		public long getRestartDelay() {
			return restartDelay;
		}

		/**
		 * Sets the delay in milliseconds before restarting the communication.
		 * 
		 * @param restartDelay
		 *            the delay in milliseconds.
		 */
		public void setRestartDelay(long restartDelay) {
			if (restartDelay < 0) {
				throw new IllegalArgumentException("The delay cannot be negative.");
			}
			this.restartDelay = restartDelay;
		}

		/**
		 * Returns the initial delay before sending of messages.
		 * 
		 * @return the initial delay in milliseconds.
		 */
		public long getSendingDelay() {
			return sendingDelay;
		}

		/**
		 * Sets the initial delay before sending of messages.
		 * 
		 * @param sendingDelay
		 *            the initial delay in milliseconds.
		 */
		public void setSendingDelay(long sendingDelay) {
			this.sendingDelay = sendingDelay;
		}

		/**
		 * Returns the maximal number of unsent messages that are stored when
		 * serial line is broken (connection failed).
		 * 
		 * @return the capacity of queue for unsent messages in offline state.
		 */
		public int getOfflineBufferCapacity() {
			return offlineBufferCapacity;
		}

		/**
		 * Sets the maximal number of unsent messages that are stored when
		 * serial line is broken (connection failed).
		 * 
		 * @param offlineBufferCapacity
		 *            the capacity of queue for unsent messages in offline
		 *            state.
		 */
		public void setOfflineBufferCapacity(int offlineBufferCapacity) {
			if (offlineBufferCapacity < 0) {
				throw new IllegalArgumentException("The capacity of offline buffer cannot be negative.");
			}
			this.offlineBufferCapacity = offlineBufferCapacity;
		}

		/**
		 * Returns clone of this configuration.
		 */
		public Configuration clone() {
			Configuration result = new Configuration(portName, baudRate);
			result.setMessageDelimiter(messageDelimiter);
			result.setRestartDelay(restartDelay);
			result.setSendingDelay(sendingDelay);
			result.setOfflineBufferCapacity(offlineBufferCapacity);
			return result;
		}
	}

	/**
	 * Default (pre-defined) name of the gateway.
	 */
	public static final String DEFAULT_ID = "serial";

	/**
	 * Topic for sending messages to serial device (the input of device).
	 */
	public static final String INPUT_TOPIC = "input";

	/**
	 * Topic for receiving messages from serial device (the output of device).
	 */
	public static final String OUTPUT_TOPIC = "output";

	/**
	 * Topic where number of restarts is published before each restart.
	 */
	public static final String RESTART_COUNT_TOPIC = "restarts";

	/**
	 * Topic where changes of connection state are published.
	 */
	public static final String CONNECTION_STATE_TOPIC = "connected";

	/**
	 * Logger.
	 */
	private static final Logger logger = Logger.getLogger(SerialGateway.class.getName());

	/**
	 * Configuration of the gateway.
	 */
	private final Configuration configuration;

	/**
	 * Thread for realizing IO operations.
	 */
	private final Thread ioThread;

	/**
	 * Lock that controls sleeping the the IO thread.
	 */
	private final Object wakeupLock = new Object();

	/**
	 * Indicator that communication should be stopped (the gateway is closing).
	 */
	private volatile boolean stopFlag;

	/**
	 * Queue of unsent messages.
	 */
	private final Queue<Message> messageQueue = new LinkedList<>();

	/**
	 * Indicates whether queue of unsent messages is in offline mode. Access to
	 * variable should be synchronized with message queue instance.
	 */
	private boolean queueInOfflineMode = true;

	/**
	 * Constructs default serial gateway.
	 * 
	 * @param portName
	 *            the name of serial port.
	 * @param baudRate
	 *            the baud rate of serial line.
	 */
	public SerialGateway(String portName, int baudRate) {
		this(new Configuration(portName, baudRate));
	}

	/**
	 * Constructs the gateway according to configuration.
	 * 
	 * @param configuration
	 *            the configuration of the gateway.
	 */
	public SerialGateway(Configuration configuration) {
		this.configuration = configuration.clone();
		ioThread = new Thread(new Runnable() {
			@Override
			public void run() {
				runIOThread();
			}
		});
		ioThread.setDaemon(true);
	}

	@Override
	protected void onStart(Map<String, Bundle> bundles) {
		synchronized (messageQueue) {
			queueInOfflineMode = false;
		}

		ioThread.start();
	}

	@Override
	protected void onStop() {
		stopFlag = true;
		synchronized (wakeupLock) {
			wakeupLock.notifyAll();
		}

		try {
			ioThread.join();
		} catch (InterruptedException ignore) {
			// ignored
		}
	}

	@Override
	protected void onPublish(Message message) {
		// store message in the queue with unsent messages
		synchronized (messageQueue) {
			messageQueue.offer(message);
			if (queueInOfflineMode) {
				finalizeUnsentMessages();
			}
		}

		// wake-up io thread
		synchronized (wakeupLock) {
			wakeupLock.notifyAll();
		}
	}

	@Override
	protected void onAddTopicFilter(String topicFilter) {
		// nothing to do
	}

	@Override
	protected void onRemoveTopicFilter(String topicFilter) {
		// nothing to do
	}

	@Override
	protected void onSaveState(Map<String, Bundle> outBundles) {
		// nothing to do
	}

	@Override
	protected boolean isValidTopicName(String topicName) {
		return INPUT_TOPIC.equals(topicName);
	}

	/**
	 * Executes code of the IO thread handling communication over serial line.
	 */
	private void runIOThread() {
		String portDescription = configuration.portName + "@" + configuration.baudRate;
		int restartCounter = 0;
		while (!stopFlag) {
			SerialPort serialPort = null;
			try {
				serialPort = new SerialPort(configuration.getPortName());
				// try open and initialize port
				try {
					serialPort.openPort();
					serialPort.setParams(configuration.getBaudRate(), SerialPort.DATABITS_8, SerialPort.STOPBITS_1,
							SerialPort.PARITY_NONE);
					logger.log(Level.INFO, "Gateway " + getId() + ": Serial port " + portDescription + " is open.");
				} catch (SerialPortException e) {
					logger.log(Level.SEVERE,
							"Gateway " + getId() + ": Unable to open and setup serial port " + portDescription + ".",
							e);
				}

				// communicate over port (if open)
				if (serialPort.isOpened()) {
					try {
						publishInApplication(new Message(CONNECTION_STATE_TOPIC, "1"));
						synchronized (messageQueue) {
							queueInOfflineMode = false;
						}
						sendAndReceiveMessages(serialPort);
					} catch (Exception e) {
						logger.log(Level.SEVERE, "Gateway " + getId() + ": Communication over serial line "
								+ portDescription + " failed.", e);
					}
					publishInApplication(new Message(CONNECTION_STATE_TOPIC, "0"));
				}
			} finally {
				synchronized (messageQueue) {
					queueInOfflineMode = true;
				}

				if (serialPort != null) {
					try {
						serialPort.closePort();
					} catch (SerialPortException ignore) {
						// nothing to do if closing failed
					}
				}
				logger.log(Level.INFO, "Gateway " + getId() + ": Serial port " + configuration.portName + " closed.");
			}

			if (!stopFlag) {
				// reduce/clean queue with unsent messages
				finalizeUnsentMessages();

				// delay between restarts
				restartCounter++;
				publishInApplication(new Message(RESTART_COUNT_TOPIC, Integer.toString(restartCounter)));
				if (configuration.restartDelay > 0) {
					synchronized (wakeupLock) {
						try {
							Thread.sleep(configuration.restartDelay);
						} catch (InterruptedException ignore) {
							// nothing to do
						}
					}
				}
			}
		}
	}

	/**
	 * Finalizes unsent messages for which there is no space in message queue
	 * (in offline mode) as processed without success.
	 */
	private void finalizeUnsentMessages() {
		synchronized (messageQueue) {
			while (messageQueue.size() > configuration.offlineBufferCapacity) {
				Message message = messageQueue.poll();
				if (message != null) {
					notifyProcessedMessage(message, false);
				}
			}
		}
	}

	/**
	 * Realizes communication over open serial line in the current session.
	 * 
	 * @param serialPort
	 *            the serial port open for communication.
	 */
	private void sendAndReceiveMessages(SerialPort serialPort) {
		// compute sleep interval with respect to baud rate.
		long nanosPerByte = Math.max((1_000_000_000L / configuration.baudRate) * (8 + 2), 100);
		final int nanosSleep = (int) (nanosPerByte % 1_000_000);
		final long millisSleep = nanosPerByte / 1_000_000;

		final int messageDelimiter = configuration.messageDelimiter;

		boolean initialSendBlocking = true;
		long startTime = MonotonicClock.currentTimeMillis();

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			while (!stopFlag) {
				// handle messages to send (eventually with a delay)
				if (!initialSendBlocking) {
					Message messageToSend = null;
					synchronized (messageQueue) {
						messageToSend = messageQueue.poll();
					}
					if (messageToSend != null) {
						serialPort.writeBytes(messageToSend.getPayload());
						serialPort.writeInt(messageDelimiter);
						notifyProcessedMessage(messageToSend, true);
						continue;
					}
				} else {
					// check time in order to unblock sending of messages
					if (MonotonicClock.currentTimeMillis() - startTime > configuration.sendingDelay) {
						initialSendBlocking = false;
					}
				}

				// check incoming data
				byte[] receivedData = serialPort.readBytes();
				if ((receivedData != null) && (receivedData.length > 0)) {
					for (byte b : receivedData) {
						int msgByte = b & 0xff;
						if (msgByte == messageDelimiter) {
							publishInApplication(new Message(OUTPUT_TOPIC, bos.toByteArray()));
							bos.reset();
						} else {
							bos.write(msgByte);
						}
					}
				} else {
					// no incoming data, sleep time required to receive a byte
					synchronized (wakeupLock) {
						try {
							wakeupLock.wait(millisSleep, nanosSleep);
						} catch (InterruptedException ignore) {
							// nothing to do
						}
					}
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Gateway " + getId() + ": Catched exception.", e);
		}
	}
}
