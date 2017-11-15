package com.hypertino.services.wallet

import com.hypertino.binders.value.{Null, Obj, Text, Value}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{BadRequest, Created, DynamicBody, EmptyBody, ErrorBody, Headers, MessagingContext, NotFound, Ok, PreconditionFailed, RequestBase, ResponseBase}
import com.hypertino.hyperbus.subscribe.Subscribable
import com.hypertino.hyperbus.transport.api.ServiceRegistrator
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import com.hypertino.service.config.ConfigLoader
import com.hypertino.user.apiref.hyperstorage._
import com.hypertino.wallet.api.{Wallet, WalletTransaction, WalletTransactionPut, WalletTransactionStatus}
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import scaldi.Module

import scala.collection.mutable
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
  private val handlers = hyperbus.subscribe(this)
  Thread.sleep(500)
  private val service = new WalletService()
  val hyperStorageContent = mutable.Map[String, (Value, Long)]()

  val failPreconditions = mutable.Map[String, (Int, AtomicInt)]()

  def onContentPut(implicit request: ContentPut): Task[ResponseBase] = {
    hbpc(request).map { rev ⇒
      if (hyperStorageContent.put(request.path, (request.body.content, rev+1)).isDefined) {
        Ok(EmptyBody)
      }
      else {
        Created(EmptyBody)
      }
    }
  }

  def onContentPatch(implicit request: ContentPatch): Task[ResponseBase] = {
    hbpc(request).map { rev ⇒
      hyperStorageContent.get(request.path) match {
        case Some(v) ⇒
          hyperStorageContent.put(request.path, (v._1 % request.body.content, rev + 1))
          Ok(EmptyBody)

        case None ⇒
          NotFound(ErrorBody("not-found"))
      }
    }
  }

  def onContentDelete(implicit request: ContentDelete): Task[ResponseBase] = {
    hbpc(request).map { rev ⇒
      if (hyperStorageContent.remove(request.path).isDefined) {
        Ok(EmptyBody)
      }
      else {
        NotFound(ErrorBody("not-found"))
      }
    }
  }

  def onContentGet(implicit request: ContentGet): Task[ResponseBase] = {
    hbpc(request).map { _ ⇒
      hyperStorageContent.get(request.path) match {
        case Some(v) ⇒ Ok(DynamicBody(v._1), headers = Headers(HyperStorageHeader.ETAG → Text("\"" + v._2 + "\"")))
        case None ⇒ NotFound(ErrorBody("not-found", Some(request.path)))
      }
    }
  }

  private def hbpc(request: RequestBase): Task[Long] = {
    val path = request.headers.hrl.query.dynamic.path.toString
    val existingRev = hyperStorageContent.get(path).map { case (_, v) ⇒
      v
    }.getOrElse {
      0l
    }
    val existingTag = "\"" + existingRev + "\""

    {
      request.headers.get("if-match").map { etag ⇒ {
        checkFailPreconditions(path)
      }.flatMap { _ ⇒
        if ((existingTag != etag.toString) && (!(etag.toString == "*" && existingRev != 0))) {
          Task.raiseError(PreconditionFailed(ErrorBody("revision")))
        } else {
          Task.now(existingRev)
        }
      }
      } getOrElse {
        Task.now(0l)
      }
    }.flatMap { r ⇒
      request.headers.get("if-none-match").map { etag ⇒ {
        checkFailPreconditions(path)
      }.flatMap { _ ⇒
        if ((existingTag == etag.toString) || (etag.toString == "*" && existingRev != 0)) {
          Task.raiseError(PreconditionFailed(ErrorBody("revision")))
        } else {
          Task.now(existingRev)
        }
      }
      } getOrElse {
        Task.now(r)
      }
    }
  }

  private def checkFailPreconditions(path: String): Task[Unit] = {
    failPreconditions.get(path).map { fp ⇒
      if (fp._2.incrementAndGet() <= fp._1) {
        Task.raiseError(PreconditionFailed(ErrorBody("fake")))
      }
      else {
        Task.unit
      }
    }.getOrElse {
      Task.unit
    }
  }

  "WalletService" should "create new wallet + transaction" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    hyperStorageContent.get(s"wallet-service/wallets/w1") shouldBe Some((Obj.from(
      "wallet_id" → "w1",
      "amount" → 10,
      "last_transaction_id" → "t1"
    ),1))

    hyperStorageContent.get(s"wallet-service/wallets/w1/transactions~/t1") shouldBe Some((Obj.from(
      "transaction_id" → "t1",
      "wallet_id" → "w1",
      "amount" → 10,
      "status" → "applied"
    ),1))
  }

  it should "update wallet with second transaction" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    val u2 = hyperbus
      .ask(WalletTransactionPut("w1", "t2", WalletTransaction("t2", "w1", -3l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u2 shouldBe a[Ok[_]]
    u2.body shouldBe a[Wallet]
    u2.body.amount shouldBe 7l

    hyperStorageContent.get(s"wallet-service/wallets/w1") shouldBe Some((Obj.from(
      "wallet_id" → "w1",
      "amount" → 7,
      "last_transaction_id" → "t2"
    ),2))

    hyperStorageContent.get(s"wallet-service/wallets/w1/transactions~/t2") shouldBe Some((Obj.from(
      "transaction_id" → "t2",
      "wallet_id" → "w1",
      "amount" → -3,
      "status" → "applied"
    ),1))
  }

  it should "complete last transaction" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    hyperStorageContent.get(s"wallet-service/wallets/w1/transactions~/t1") shouldBe Some((Obj.from(
      "transaction_id" → "t1",
      "wallet_id" → "w1",
      "amount" → 10,
      "status" → "applied"
    ),1))

    hyperStorageContent.put(s"wallet-service/wallets/w1/transactions~/t1", (Obj.from(
      "transaction_id" → "t1",
      "wallet_id" → "w1",
      "amount" → 10,
      "status" → "new"
    ),1l))

    val u2 = hyperbus
      .ask(WalletTransactionPut("w1", "t2", WalletTransaction("t2", "w1", -3l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u2 shouldBe a[Ok[_]]
    u2.body shouldBe a[Wallet]
    u2.body.amount shouldBe 7l

    hyperStorageContent.get(s"wallet-service/wallets/w1") shouldBe Some((Obj.from(
      "wallet_id" → "w1",
      "amount" → 7,
      "last_transaction_id" → "t2"
    ),2))

    hyperStorageContent.get(s"wallet-service/wallets/w1/transactions~/t1") shouldBe Some((Obj.from(
      "transaction_id" → "t1",
      "wallet_id" → "w1",
      "amount" → 10,
      "status" → "applied"
    ),1))

    hyperStorageContent.get(s"wallet-service/wallets/w1/transactions~/t2") shouldBe Some((Obj.from(
      "transaction_id" → "t2",
      "wallet_id" → "w1",
      "amount" → -3,
      "status" → "applied"
    ),1))
  }

  it should "retry transaction if precondition failed" in {
    val u = hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w1", 10l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u shouldBe a[Created[_]]
    u.body shouldBe a[Wallet]

    failPreconditions.put("wallet-service/wallets/w1", (2, AtomicInt(0)))

    val u2 = hyperbus
      .ask(WalletTransactionPut("w1", "t2", WalletTransaction("t2", "w1", -3l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .futureValue
    u2 shouldBe a[Ok[_]]
    u2.body shouldBe a[Wallet]
    u2.body.amount shouldBe 7l

    hyperStorageContent.get(s"wallet-service/wallets/w1") shouldBe Some((Obj.from(
      "wallet_id" → "w1",
      "amount" → 7,
      "last_transaction_id" → "t2"
    ),2))

    hyperStorageContent.get(s"wallet-service/wallets/w1/transactions~/t2") shouldBe Some((Obj.from(
      "transaction_id" → "t2",
      "wallet_id" → "w1",
      "amount" → -3,
      "status" → "applied"
    ),1))

    failPreconditions("wallet-service/wallets/w1")._2.get shouldBe 3
  }

  it should "validate transaction fields" in {
    hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("", "w1", 10l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue shouldBe a[BadRequest[_]]

    hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t2", "w1", 10l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue shouldBe a[BadRequest[_]]

    hyperbus
      .ask(WalletTransactionPut("w1", "t1", WalletTransaction("t1", "w2", 10l, WalletTransactionStatus.NEW, Null)))
      .runAsync
      .failed
      .futureValue shouldBe a[BadRequest[_]]
  }

  override def afterAll() {
    service.stopService(false, 10.seconds).futureValue
    hyperbus.shutdown(10.seconds).runAsync.futureValue
  }

  override def beforeEach(): Unit = {
    hyperStorageContent.clear()
    failPreconditions.clear()
  }
}
