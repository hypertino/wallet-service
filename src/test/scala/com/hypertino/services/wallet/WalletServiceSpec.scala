package com.hypertino.services.wallet

import com.hypertino.binders.value.{Lst, Null, Obj, Text, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{BadRequest, Conflict, Created, MessagingContext, Ok}
import com.hypertino.hyperbus.subscribe.Subscribable
import com.hypertino.hyperbus.transport.api.ServiceRegistrator
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import com.hypertino.mock.hyperstorage.HyperStorageMock
import com.hypertino.service.config.ConfigLoader
import com.hypertino.wallet.api._
import com.typesafe.config.Config
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import scaldi.Module

import scala.concurrent.duration._

class WalletServiceSpec extends FlatSpec with Module with BeforeAndAfterAll with BeforeAndAfterEach with ScalaFutures with Matchers with Subscribable {
  implicit val patience = PatienceConfig(scaled(Span(60, Seconds)))
  private implicit val scheduler = monix.execution.Scheduler.Implicits.global
  private implicit val mcx = MessagingContext.empty
  bind [Config] to ConfigLoader()
  bind [Scheduler] to scheduler
  bind [Hyperbus] to injected[Hyperbus]
  bind [ServiceRegistrator] to DummyRegistrator

  private val hyperbus = inject[Hyperbus]
  Thread.sleep(500)
  private val service = new WalletService()
  private val hyperStorageMock = new HyperStorageMock(hyperbus, scheduler)
  import hyperStorageMock._
  hyperbus.startServices()

  val time: Long = System.currentTimeMillis()

  "WalletService" should "create new wallet + transaction" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    hyperStorageContent.get(s"wallet-service/wallets/w1") shouldBe Some((Obj.from(
      "wallet_id" → "w1",
      "amount" → 10,
      "last_transaction_id" → "t1",
      "updated_at" → time
    ),1))

    hyperStorageContent.get(s"wallet-service/wallets/w1/transactions~") shouldBe Some((Obj.from("t1" -> Obj.from(
      "transaction_id" → "t1",
      "wallet_id" → "w1",
      "amount" → 10,
      "status" → "applied",
      "created_at" → time
    )),2))

    val t = hyperbus
      .ask(WalletTransactionGet("w1", "t1"))
      .runAsync
      .futureValue

    t shouldBe a[Ok[_]]
    t.body shouldBe WalletTransaction("t1", "w1", 10l, time, None, None, WalletTransactionStatus.APPLIED, Null)
  }

  it should "update wallet with second transaction" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    val u2 = hyperbus
      .ask(WalletTransactionPut("w1", "t2", WalletTransaction("t2", "w1", -3l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u2 shouldBe a[Ok[_]]
    u2.body shouldBe a[Wallet]
    u2.body.amount shouldBe 7l

    hyperStorageContent.get(s"wallet-service/wallets/w1") shouldBe Some((Obj.from(
      "wallet_id" → "w1",
      "amount" → 7,
      "last_transaction_id" → "t2",
      "updated_at" → time
    ),2))

    val transactions = hyperStorageContent(s"wallet-service/wallets/w1/transactions~")
    transactions._2 shouldBe 2
    transactions._1.dynamic.t2 shouldBe Obj.from(
      "transaction_id" → "t2",
      "wallet_id" → "w1",
      "amount" → -3,
      "status" → "applied",
      "created_at" → time
    )
  }

  it should "complete last transaction" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    val transactions = hyperStorageContent(s"wallet-service/wallets/w1/transactions~")
    transactions._2 shouldBe 2
    transactions._1.dynamic.t1 shouldBe Obj.from(
      "transaction_id" → "t1",
      "wallet_id" → "w1",
      "amount" → 10,
      "status" → "applied",
      "created_at" → time
    )

    hyperStorageContent.put("wallet-service/wallets/w1/transactions~", (Obj.from("t1" -> Obj.from(
      "transaction_id" → "t1",
      "wallet_id" → "w1",
      "amount" → 10,
      "status" → "new",
      "created_at" → time
    )),1l))

    val u2 = hyperbus
      .ask(WalletTransactionPut("w1", "t2", WalletTransaction("t2", "w1", -3l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u2 shouldBe a[Ok[_]]
    u2.body shouldBe a[Wallet]
    u2.body.amount shouldBe 7l

    hyperStorageContent.get(s"wallet-service/wallets/w1") shouldBe Some((Obj.from(
      "wallet_id" → "w1",
      "amount" → 7,
      "last_transaction_id" → "t2",
      "updated_at" → time
    ),2))

    val transactions2 = hyperStorageContent(s"wallet-service/wallets/w1/transactions~")
    transactions2._2 shouldBe 3
    transactions2._1.dynamic.t1 shouldBe Obj.from(
      "transaction_id" → "t1",
      "wallet_id" → "w1",
      "amount" → 10,
      "status" → "applied",
      "created_at" → time
    )

    transactions2._1.dynamic.t2 shouldBe Obj.from(
      "transaction_id" → "t2",
      "wallet_id" → "w1",
      "amount" → -3,
      "status" → "applied",
      "created_at" → time
    )
  }

  it should "retry transaction if precondition failed" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    failPreconditions.put("wallet-service/wallets/w1", (2, AtomicInt(0)))

    val u2 = hyperbus
      .ask(WalletTransactionPut("w1", "t2", WalletTransaction("t2", "w1", -3l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u2 shouldBe a[Ok[_]]
    u2.body shouldBe a[Wallet]
    u2.body.amount shouldBe 7l

    hyperStorageContent.get(s"wallet-service/wallets/w1") shouldBe Some((Obj.from(
      "wallet_id" → "w1",
      "amount" → 7,
      "last_transaction_id" → "t2",
      "updated_at" → time
    ),2))

    val transactions2 = hyperStorageContent(s"wallet-service/wallets/w1/transactions~")
    transactions2._2 shouldBe 2
    transactions2._1.dynamic.t2 shouldBe Obj.from(
      "transaction_id" → "t2",
      "wallet_id" → "w1",
      "amount" → -3,
      "status" → "applied",
      "created_at" → time
    )

    failPreconditions("wallet-service/wallets/w1")._2.get shouldBe 3
  }

  it should "validate transaction fields" in {
    hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("", "w1", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue shouldBe a[BadRequest[_]]

    hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t2", "w1", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue shouldBe a[BadRequest[_]]

    hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w2", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue shouldBe a[BadRequest[_]]
  }

  it should "validate wallet bounds" in {
    hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", -1, time, Some(0), None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue shouldBe a[Conflict[_]]

    hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10, time, None, Some(9), WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue shouldBe a[Conflict[_]]
  }

  it should "protect from duplicate transaction" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    val u2 = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u2 shouldBe a[Ok[_]]
    u2.body shouldBe a[Wallet]
    u2.body.amount shouldBe 10l

    val u3 = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 11l, time, None, None, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue
    u3 shouldBe a[Conflict[_]]
  }

  override def afterAll() {
    service.stopService(false, 10.seconds).futureValue
    hyperbus.shutdown(10.seconds).runAsync.futureValue
  }

  override def beforeEach(): Unit = {
    hyperStorageMock.reset()
  }
}
