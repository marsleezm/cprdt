package swift.application.social;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import swift.crdt.CRDTIdentifier;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.SetIds;
import swift.crdt.SetMsg;
import swift.crdt.SetTxnLocalId;
import swift.crdt.SetTxnLocalMsg;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

// implements the social network functionality
// see wsocial_srv.h

public class SwiftSocial {

    public static final int RETRY_DELAY_MS = 500;
    private static Logger logger = Logger.getLogger("swift.social");

    // FIXME Add sessions? Local login possible? Cookies?
    private User currentUser;
    private Swift server;
    private final IsolationLevel isolationLevel;
    private final CachePolicy cachePolicy;
    private final ObjectUpdatesListener updatesSubscriber;
    private final boolean asyncCommit;

    public SwiftSocial(Swift clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean subscribeUpdates, boolean asyncCommit) {
        server = clientServer;
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.updatesSubscriber = subscribeUpdates ? TxnHandle.UPDATES_SUBSCRIBER : null;
        this.asyncCommit = asyncCommit;
    }

    // FIXME Return type integer encoding error msg?
    boolean login(final String loginName, final String passwd) {
        logger.info("Got login request from user " + loginName);

        // Check if user is already logged in
        if (currentUser != null) {
            if (loginName.equals(currentUser)) {
                logger.info(loginName + " is already logged in");
                return true;
            } else {
                logger.info("Need to log out user " + currentUser.loginName + " first!");
                return false;
            }
        }

        runRetryableTask(new RetryableTask() {
            @Override
            public boolean run() {
                TxnHandle txn = null;
                try {
                    // Check if user is known at all
                    // FIXME Is login possible in offline mode?

                    // ATTENZIONE ATTENZIONE, HACK!! Shall we perhaps do it at
                    // client
                    // startup? I am not changing it now to make experiments
                    // comparable.
                    final CachePolicy loginCachePolicy;
                    if (isolationLevel == IsolationLevel.SNAPSHOT_ISOLATION && cachePolicy == CachePolicy.CACHED) {
                        loginCachePolicy = CachePolicy.MOST_RECENT;
                    } else {
                        loginCachePolicy = cachePolicy;
                    }
                    txn = server.beginTxn(isolationLevel, loginCachePolicy, true);
                    @SuppressWarnings("unchecked")
                    User user = (User) (txn.get(NamingScheme.forUser(loginName), false, RegisterVersioned.class,
                            updatesSubscriber)).getValue();

                    // Check password
                    // FIXME We actually need an external authentification
                    // mechanism, as
                    // clients cannot be trusted.
                    // In Walter, authentification is done on server side,
                    // within the
                    // data center. Moving password (even if hashed) to the
                    // client is a
                    // security breach.
                    if (user != null) {
                        if (user.password.equals(passwd)) {
                            currentUser = user;
                            logger.info(loginName + " successfully logged in");
                            commitTxn(txn);
                            return true;
                        } else {
                            logger.info("Wrong password for " + loginName);
                        }
                    } else {
                        logger.info("User has not been registered " + loginName);
                    }
                } catch (NetworkException e) {
                    return true;
                } catch (WrongTypeException e) {
                    // should not happen
                    e.printStackTrace();
                } catch (NoSuchObjectException e) {
                    logger.info("User " + loginName + " is not known");
                } catch (VersionNotFoundException e) {
                    // should not happen
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                return false;
            }
        });

        return false;
    }

    void logout(String loginName) {
        currentUser = null;
        // FIXME End session? handle cookies?
        logger.info(loginName + " successfully logged out");
    }

    // FIXME Return error code?
    void registerUser(final String loginName, final String passwd, final String fullName, final long birthday,
            final long date) {
        logger.info("Got registration request for " + loginName);
        // FIXME How do we guarantee unique login names?
        // WalterSocial suggests using dedicated (non-replicated) login server.

        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, CachePolicy.STRICTLY_MOST_RECENT, false);
            User newUser = registerUser(txn, loginName, passwd, fullName, birthday, date);
            logger.info("Registered user: " + newUser);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    User registerUser(final TxnHandle txn, final String loginName, final String passwd, final String fullName,
            final long birthday, final long date) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        // FIXME How do we guarantee unique login names?
        // WalterSocial suggests using dedicated (non-replicated) login server.

        RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(loginName), true,
                RegisterVersioned.class, null);

        User newUser = new User(loginName, passwd, fullName, birthday, true);
        reg.set(newUser);

        // Construct the associated sets with messages, friends etc.
        txn.get(newUser.msgList, true, SetMsg.class, null);
        txn.get(newUser.eventList, true, SetMsg.class, null);
        txn.get(newUser.friendList, true, SetIds.class, null);
        txn.get(newUser.inFriendReq, true, SetIds.class, null);
        txn.get(newUser.outFriendReq, true, SetIds.class, null);

        // Create registration event for user
        Message newEvt = new Message(fullName + " has registered!", loginName, date);
        writeMessage(txn, newEvt, newUser.eventList);

        return newUser;
    }

    void updateUser(boolean status, String fullName, long birthday) {
        logger.info("Update user data for " + this.currentUser.loginName);
        this.currentUser.active = status;
        this.currentUser.fullName = fullName;
        this.currentUser.birthday = birthday;

        final String name = this.currentUser.loginName;

        runRetryableTask(new RetryableTask() {

            @Override
            public boolean run() {
                TxnHandle txn = null;
                try {
                    txn = server.beginTxn(isolationLevel, cachePolicy, false);
                    RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(name), true,
                            RegisterVersioned.class, updatesSubscriber);
                    reg.set(currentUser);
                    commitTxn(txn);
                } catch (NetworkException x) {
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                return false;
            }
        });
    }

    @SuppressWarnings("unchecked")
    User read(final String name, final Collection<Message> msgs, final Collection<Message> evnts) {
        logger.info("Get site report for " + name);

        final AtomicReference<User> refUser = new AtomicReference<User>();
        runRetryableTask(new RetryableTask() {
            @Override
            public boolean run() {
                TxnHandle txn = null;
                User user = null;
                try {
                    txn = server.beginTxn(isolationLevel, cachePolicy, true);
                    RegisterTxnLocal<User> reg = (RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(name), false,
                            RegisterVersioned.class);
                    user = reg.getValue();
                    msgs.addAll(((SetTxnLocalMsg) txn.get(user.msgList, false, SetMsg.class, updatesSubscriber))
                            .getValue());
                    evnts.addAll(((SetTxnLocalMsg) txn.get(user.eventList, false, SetMsg.class, updatesSubscriber))
                            .getValue());
                    commitTxn(txn);
                } catch (NetworkException x) {
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                refUser.set(user);
                return true;
            }
        });
        return refUser.get();
    }

    // FIXME return error code?
    void postMessage(final String receiverName, String msg, long date) {
        logger.info("Post status msg from " + this.currentUser.loginName + " for " + receiverName);
        final Message newMsg = new Message(msg, this.currentUser.loginName, date);
        final Message newEvt = new Message(currentUser.loginName + " has posted a message  to " + receiverName,
                this.currentUser.loginName, date);

        runRetryableTask(new RetryableTask() {
            public boolean run() {
                TxnHandle txn = null;
                try {
                    txn = server.beginTxn(isolationLevel, cachePolicy, false);
                    User receiver = ((RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(receiverName), false,
                            RegisterVersioned.class)).getValue();
                    writeMessage(txn, newMsg, receiver.msgList);
                    writeMessage(txn, newEvt, currentUser.eventList);
                    commitTxn(txn);
                } catch (NetworkException x) {
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                return false;
            };
        });
    }

    void updateStatus(String msg, long date) {
        logger.info("Update status for " + this.currentUser.loginName);
        final Message newMsg = new Message(msg, this.currentUser.loginName, date);
        final Message newEvt = new Message(currentUser.loginName + " has an updated status",
                this.currentUser.loginName, date);
        runRetryableTask(new RetryableTask() {

            @Override
            public boolean run() {
                TxnHandle txn = null;
                try {
                    txn = server.beginTxn(isolationLevel, cachePolicy, false);
                    writeMessage(txn, newMsg, currentUser.msgList);
                    writeMessage(txn, newEvt, currentUser.eventList);
                    commitTxn(txn);
                    // TODO Broadcast update to friends
                } catch (NetworkException x) {
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                return false;
            }
        });
    }

    void answerFriendRequest(final String requester, final boolean accept) {
        logger.info("Answered friend request from " + this.currentUser.loginName + " for " + requester);
        final String name = this.currentUser.loginName;
        runRetryableTask(new RetryableTask() {
            @Override
            public boolean run() {
                TxnHandle txn = null;
                try {
                    txn = server.beginTxn(isolationLevel, cachePolicy, false);
                    // Obtain data of requesting user
                    User other = ((RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(requester), false,
                            RegisterVersioned.class)).getValue();

                    // Remove information for request
                    SetTxnLocalId inFriendReq = (SetTxnLocalId) txn.get(currentUser.inFriendReq, false, SetIds.class,
                            updatesSubscriber);
                    inFriendReq.remove(NamingScheme.forUser(requester));
                    SetTxnLocalId outFriendReq = (SetTxnLocalId) txn.get(other.outFriendReq, false, SetIds.class,
                            updatesSubscriber);
                    outFriendReq.remove(NamingScheme.forUser(name));

                    // Befriend if accepted
                    if (accept) {
                        SetTxnLocalId friends = (SetTxnLocalId) txn.get(currentUser.friendList, false, SetIds.class,
                                updatesSubscriber);
                        friends.insert(NamingScheme.forUser(requester));
                        SetTxnLocalId requesterFriends = (SetTxnLocalId) txn.get(other.friendList, false, SetIds.class,
                                updatesSubscriber);
                        requesterFriends.insert(NamingScheme.forUser(name));
                    }
                    commitTxn(txn);
                } catch (NetworkException x) {
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                return false;
            }
        });
    }

    void sendFriendRequest(final String receiverName) {
        logger.info("Sending friend request from to " + receiverName);

        runRetryableTask(new RetryableTask() {

            @Override
            public boolean run() {
                TxnHandle txn = null;
                try {
                    txn = server.beginTxn(isolationLevel, cachePolicy, false);
                    // Obtain data of friend
                    User other = ((RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(receiverName), false,
                            RegisterVersioned.class)).getValue();

                    // Add data for request
                    SetTxnLocalId inFriendReq = (SetTxnLocalId) txn.get(other.inFriendReq, false, SetIds.class,
                            updatesSubscriber);
                    inFriendReq.insert(NamingScheme.forUser(currentUser.loginName));
                    SetTxnLocalId outFriendReq = (SetTxnLocalId) txn.get(currentUser.outFriendReq, false, SetIds.class,
                            updatesSubscriber);
                    outFriendReq.insert(NamingScheme.forUser(receiverName));

                    commitTxn(txn);
                } catch (NetworkException x) {
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                return false;
            }
        });
    }

    void befriend(final String receiverName) {
        logger.info("Befriending " + receiverName);

        final String name = this.currentUser.loginName;
        runRetryableTask(new RetryableTask() {

            @Override
            public boolean run() {
                TxnHandle txn = null;
                try {
                    txn = server.beginTxn(isolationLevel, cachePolicy, false);
                    // Obtain new friend's data
                    User friend = ((RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(receiverName), false,
                            RegisterVersioned.class)).getValue();

                    // Register him as my friend
                    SetTxnLocalId friends = (SetTxnLocalId) txn.get(currentUser.friendList, false, SetIds.class,
                            updatesSubscriber);
                    friends.insert(NamingScheme.forUser(receiverName));

                    // Register me as his friend
                    SetTxnLocalId requesterFriends = (SetTxnLocalId) txn.get(friend.friendList, false, SetIds.class,
                            updatesSubscriber);
                    requesterFriends.insert(NamingScheme.forUser(name));
                    commitTxn(txn);
                } catch (NetworkException x) {
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                return false;
            }
        });
    }

    Set<Friend> readFriendList(final String name) {
        logger.info("Get friends of " + name);

        final Set<Friend> friends = new HashSet<Friend>();
        runRetryableTask(new RetryableTask() {

            @Override
            public boolean run() {
                TxnHandle txn = null;
                try {
                    txn = server.beginTxn(isolationLevel, cachePolicy, true);
                    // Obtain user data
                    User user = ((RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(name), false,
                            RegisterVersioned.class, updatesSubscriber)).getValue();

                    Set<CRDTIdentifier> friendIds = ((SetTxnLocalId) txn.get(user.friendList, false, SetIds.class,
                            updatesSubscriber)).getValue();
                    for (CRDTIdentifier f : friendIds) {
                        User u = ((RegisterTxnLocal<User>) txn.get(NamingScheme.forUser(name), false,
                                RegisterVersioned.class, updatesSubscriber)).getValue();
                        friends.add(new Friend(u.fullName, f));
                    }
                    commitTxn(txn);
                } catch (NetworkException x) {
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (txn != null && !txn.getStatus().isTerminated()) {
                        txn.rollback();
                    }
                }
                return false;
            }
        });
        return friends;
    }

    private void writeMessage(TxnHandle txn, Message msg, CRDTIdentifier set) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        SetTxnLocalMsg messages = (SetTxnLocalMsg) txn.get(set, false, SetMsg.class, updatesSubscriber);
        messages.insert(msg);
    }

    private void commitTxn(final TxnHandle txn) {
        if (asyncCommit) {
            txn.commitAsync(null);
        } else {
            txn.commit();
        }
    }

    private void runRetryableTask(final RetryableTask task) {
        boolean retry;
        do {
            retry = task.run();
            if (retry) {
                logger.warning("retrying swift social operation");
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException e) {
                }
            }
        } while (retry);
    }

    interface RetryableTask {
        boolean run();
    }
}
