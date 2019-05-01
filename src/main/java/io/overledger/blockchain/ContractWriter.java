package io.overledger.blockchain;

import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Int;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.Utf8String;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.RawTransaction;
import org.web3j.crypto.TransactionEncoder;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.request.Transaction;
import org.web3j.protocol.core.methods.response.EthCall;
import org.web3j.protocol.core.methods.response.EthSendTransaction;
import org.web3j.protocol.http.HttpService;
import org.web3j.utils.Numeric;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ContractWriter {

    private final static String KEY_RPC_HOST = "rpchost";
    private final static String KEY_PRIVATEKEY = "privatekey";
    private final static String KEY_OFFICE = "officeAddress";
    private final static String KEY_CONTRACT = "contract";
    private static final BigInteger DEFAULT_TEST_GAS_LIMIT = new BigInteger("4712388");
    private Web3j web3j = null;
    private ECKeyPair key = null;
    private String officeAddress = null;
    private String contractAddress = null;

    private ContractWriter(InputStream inputStream) {
        try {
            this.loadContext(inputStream);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private BigInteger getNonce() {
        BigInteger nonce = BigInteger.ZERO;
        if (null != this.key && null != this.web3j) {
            try {
                BigInteger blockNumber = this.web3j.ethBlockNumber().send().getBlockNumber();
                nonce = this.web3j.ethGetTransactionCount(Credentials.create(this.key).getAddress(), DefaultBlockParameter.valueOf(blockNumber)).send().getTransactionCount();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        return nonce;
    }

    private void loadContext(Properties properties) {
        String keyString = properties.getProperty(KEY_PRIVATEKEY);
        if (null != keyString) {
            this.key = ECKeyPair.create(DatatypeConverter.parseHexBinary(keyString));
        }
        String host = properties.getProperty(KEY_RPC_HOST);
        if (null != host) {
            HttpService httpService = new HttpService(host);
            this.web3j = Web3j.build(httpService);;
        }
        this.officeAddress = properties.getProperty(KEY_OFFICE);
        this.contractAddress = properties.getProperty(KEY_CONTRACT);
    }

    private String sendFunction(Function function) throws ExecutionException, InterruptedException {
        String encodedFunction = FunctionEncoder.encode(function);
        RawTransaction rawTransaction = RawTransaction.createTransaction(this.getNonce(), new BigInteger("125"), DEFAULT_TEST_GAS_LIMIT, this.contractAddress, encodedFunction);
        byte signedMessage[] = TransactionEncoder.signMessage(rawTransaction, Credentials.create(this.key));
        String hexValue = Numeric.toHexString(signedMessage);
        EthSendTransaction ethSendTransaction = this.web3j.ethSendRawTransaction(hexValue).sendAsync().get();
        if (null != ethSendTransaction.getError()) {
            System.err.println(ethSendTransaction.getError().getMessage());
            return null;
        } else {
            return ethSendTransaction.getTransactionHash();
        }
    }
    
    private String sendFunction(Function function, BigInteger nonce) throws ExecutionException, InterruptedException {
        String encodedFunction = FunctionEncoder.encode(function);
        RawTransaction rawTransaction = RawTransaction.createTransaction(nonce, new BigInteger("125"), DEFAULT_TEST_GAS_LIMIT, this.contractAddress, encodedFunction);
        byte signedMessage[] = TransactionEncoder.signMessage(rawTransaction, Credentials.create(this.key));
        String hexValue = Numeric.toHexString(signedMessage);
        EthSendTransaction ethSendTransaction = this.web3j.ethSendRawTransaction(hexValue).sendAsync().get();
        if (null != ethSendTransaction.getError()) {
            System.err.println(ethSendTransaction.getError().getMessage());
            return null;
        } else {
            return ethSendTransaction.getTransactionHash();
        }
    }

    private List<Type> queryFunction(Function function) throws IOException {
        String encodedFunction = FunctionEncoder.encode(function);
        Transaction transaction = Transaction.createEthCallTransaction(Credentials.create(this.key).getAddress(), this.contractAddress, encodedFunction);
        EthCall ethCall = this.web3j.ethCall(transaction, DefaultBlockParameterName.LATEST).send();
        if (null != ethCall.getError()) {
            System.err.println(ethCall.getError().getMessage());
            return null;
        } else {
            return FunctionReturnDecoder.decode(ethCall.getValue(), function.getOutputParameters());
        }
    }

    private String getTypeAsString(List<Type> someTypes) {
        if (null != someTypes) {
            return someTypes.get(0).getValue().toString();
        } else {
            return null;
        }
    }

    public void loadContext(InputStream inputStream) throws IOException {
        if (null != inputStream) {
            Properties properties = new Properties();
            properties.load(inputStream);
            loadContext(properties);
        }
    }

    public String[] sendData(String category[], BigInteger value[], BigInteger timestamp[]) throws ExecutionException, InterruptedException {
    	String result[] = new String[category.length];
    	BigInteger nonce = this.getNonce();
    	for (int i=0; i<result.length; i++) {
    		Function function = new Function(
                    "addData",
                    Arrays.asList(new Address(this.officeAddress), new Utf8String(category[i]), new Int(value[i]), new Uint256(timestamp[i])),
                    Collections.emptyList()
            );
            String txhash = this.sendFunction(function, nonce);
            if (null !=txhash) {
            	result[i] = txhash;
            	nonce = nonce.add(BigInteger.ONE);
            }
    	}
        return result;
    }

    public String createOffice(String officeName) throws ExecutionException, InterruptedException {
        Function function = new Function(
                "addOffice",
                Arrays.asList(new Utf8String(officeName)),
                Collections.emptyList()
        );
        return this.sendFunction(function);
    }

    public String addDevice(String deviceAddress, String deviceName) throws ExecutionException, InterruptedException {
        Function function = new Function(
                "addDevice",
                Arrays.asList(new Address(deviceAddress), new Utf8String(deviceName)),
                Collections.emptyList()
        );
        return this.sendFunction(function);
    }

    public String getOffice() throws IOException {
        Function function = new Function(
                "getOffice",
                Collections.emptyList(),
                Arrays.asList(new TypeReference<Utf8String>() {
                })
        );
        List<Type> someTypes = this.queryFunction(function);
        return this.getTypeAsString(someTypes);
    }

    public String getDevice(String deviceAddress) throws IOException {
        Function function = new Function(
                "getDevice",
                Arrays.asList(new Address(deviceAddress)),
                Arrays.asList(new TypeReference<Utf8String>() {
                })
        );
        List<Type> someTypes = this.queryFunction(function);
        return this.getTypeAsString(someTypes);
    }

    public String getDataListSize(String officeAddress, String deviceAddress) throws IOException {
        Function function = new Function(
                "getDataListSize",
                Arrays.asList(new Address(officeAddress), new Address(deviceAddress)),
                Arrays.asList(new TypeReference<Uint256>() {
                })
        );
        List<Type> someTypes = this.queryFunction(function);
        return this.getTypeAsString(someTypes);
    }

    public List<String> getData(String officeAddress, String deviceAddress, BigInteger index) throws IOException {
        Function function = new Function(
                "getData",
                Arrays.asList(new Address(officeAddress), new Address(deviceAddress), new Uint256(index)),
                Arrays.asList(new TypeReference<Utf8String>() {}, new TypeReference<Int>() {}, new TypeReference<Uint256>() {})
        );
        List<Type> someTypes = this.queryFunction(function);
        if (null != someTypes) {
            return someTypes.stream().map(type -> type.getValue().toString()).collect(Collectors.toList());
        } else {
            return null;
        }
    }

    public static ContractWriter newInstance(InputStream inputStream) {
        return new ContractWriter(inputStream);
    }

}
