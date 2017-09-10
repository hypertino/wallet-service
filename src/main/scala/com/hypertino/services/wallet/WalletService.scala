package com.hypertino.services.wallet

import java.math.MathContext

import com.hypertino.binders.value._
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{BadRequest, Created, DynamicBody, ErrorBody, GatewayTimeout, Header, HeadersMap, MessagingContext, NotFound, Ok, PreconditionFailed, Response}
import com.hypertino.hyperbus.serialization.SerializationOptions
import com.hypertino.hyperbus.subscribe.Subscribable
import com.hypertino.service.control.api.Service
import com.hypertino.user.apiref.hyperstorage.{ContentGet, ContentPatch, ContentPut}
import com.hypertino.wallet.api._
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import com.typesafe.scalalogging.StrictLogging
import scaldi.{Injectable, Injector}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class WalletService(implicit val injector: Injector) extends Service with Injectable with Subscribable with StrictLogging {
  protected implicit val scheduler = inject[Scheduler]
  protected val hyperbus = inject[Hyperbus]
  protected val handlers = hyperbus.subscribe(this, logger)
  protected final val mc = MathContext.DECIMAL64

  protected implicit val so = SerializationOptions.default
  import so._

  logger.info(s"${getClass.getName} started")

  def onWalletGet(implicit request: WalletGet): Task[Ok[Wallet]] = {
    hyperbus
      .ask(ContentGet(hyperStorageWalletPath(request.walletId)))
      .map {
        case ok@Ok(_: DynamicBody, _) ⇒
          val revision = ok.headers(Header.REVISION)
          Ok(ok.body.content.to[Wallet], $headersMap=HeadersMap(Header.REVISION → revision))
      }
  }

  def onWalletTransactionPut(implicit request: WalletTransactionPut): Task[Response[Wallet]] = {
    validateTransaction(request).flatMap { amount ⇒
      saveTransaction(request.body).flatMap { _ ⇒
        val LIMIT = 5
        val counter = AtomicInt(LIMIT) // do maximum 5 retries on conflict
        updateOrCreateWallet(amount, request.body)
          .onErrorRestartIf { e ⇒
            e.isInstanceOf[PreconditionFailed[_]] && counter.decrementAndGet() > 0
          }
          .materialize
          .flatMap {
            case Success(r) ⇒
              saveTransactionStatus(request.body, WalletTransactionStatus.APPLIED).map { _ ⇒
                r
              }
            case Failure(_: PreconditionFailed[_]) ⇒ Task.raiseError(GatewayTimeout(ErrorBody("wallet-update-failed", Some(s"Wallet update(s) conflit limit is reached ($LIMIT)"))))
            case Failure(e) ⇒ Task.raiseError(e)
          }
      }
    }
  }

  protected def updateOrCreateWallet(amount: Long, transaction: WalletTransaction)
                                    (implicit mcx: MessagingContext): Task[Response[Wallet]] = {
    selectWallet(transaction.walletId).flatMap { walletOption ⇒
      walletOption
        .map(ww ⇒ applyLastTransaction(ww.wallet))
        .getOrElse(Task.unit)
        .flatMap { _ ⇒
          walletOption match {
            case Some(wallet) ⇒
              updateWallet(wallet, amount, transaction)
            case None ⇒
              createWallet(transaction)
          }
      }
    }
  }

  protected def updateWallet(walletWithRevision: WalletWithRevision, amount: Long, transaction: WalletTransaction)
                            (implicit mcx: MessagingContext): Task[Response[Wallet]] = {
    val newAmount = walletWithRevision.wallet.amount + amount
    val newWallet = walletWithRevision.wallet.copy(
      amount=newAmount,
      lastTransactionId=transaction.transactionId
    )

    val r = ContentPatch(hyperStorageWalletPath(transaction.walletId), DynamicBody(
      Obj.from("last_transaction_id" → transaction.transactionId, "amount" → newAmount)
    ), $headersMap=HeadersMap("If-Match" →  Text("\""+ walletWithRevision.revision.toString + "\"")))

    hyperbus
      .ask(r)
      .map { _ ⇒
        Ok(newWallet)
      }
  }

  protected def createWallet(transaction: WalletTransaction)
                            (implicit mcx: MessagingContext): Task[Response[Wallet]] = {
    val newWallet = Wallet(
      walletId=transaction.walletId,
      amount=transaction.amount,
      lastTransactionId=transaction.transactionId
    )

    val r = ContentPut(hyperStorageWalletPath(transaction.walletId), DynamicBody(
      newWallet.toValue
    ), $headersMap=HeadersMap("If-Match" → "\"\""))

    hyperbus
      .ask(r)
      .map { _ ⇒
        Created(newWallet)
      }
  }

  protected def selectWallet(walletId: String)
                                    (implicit mcx: MessagingContext): Task[Option[WalletWithRevision]] = {
    onWalletGet(WalletGet(walletId))
      .materialize
      .flatMap {
        case Success(ok @ Ok(w, _)) ⇒ Task.now(Some(WalletWithRevision(w, ok.headers(Header.REVISION).toLong)))
        case Failure(NotFound(_, _)) ⇒ Task.now(None)
        case Failure(e) ⇒ Task.raiseError(e)
      }
  }

  protected def applyLastTransaction(wallet: Wallet)
                                    (implicit mcx: MessagingContext): Task[Unit] = {

    getTransaction(wallet.walletId, wallet.lastTransactionId).flatMap {
      case Some(transaction) ⇒
        if (transaction.status == WalletTransactionStatus.NEW)
          saveTransactionStatus(transaction, WalletTransactionStatus.APPLIED)
        else
          Task.unit

      case None ⇒
        Task.unit
    }
  }

  protected def saveTransaction(transaction: WalletTransaction)
                               (implicit mcx: MessagingContext): Task[Unit] = {
    hyperbus
      .ask(ContentPut(hyperStorageWalletTransactionPath(transaction.walletId, transaction.transactionId), DynamicBody(
        transaction.toValue
      )))
      .map { _ ⇒ }
  }

  protected def saveTransactionStatus(transaction: WalletTransaction, status: String)
                                     (implicit mcx: MessagingContext): Task[Unit] = {
    hyperbus
      .ask(ContentPatch(hyperStorageWalletTransactionPath(transaction.walletId, transaction.transactionId), DynamicBody(
        Obj.from("status" → status)
      )))
      .map { _ ⇒ }
  }

  protected def getTransaction(walletId: String, transactionId: String)
                                       (implicit mcx: MessagingContext): Task[Option[WalletTransaction]] = {
    hyperbus
      .ask(ContentGet(hyperStorageWalletTransactionPath(walletId, transactionId)))
      .materialize
      .flatMap {
        case Success(ok@Ok(_: DynamicBody, _)) ⇒
          val revision = ok.headers(Header.REVISION)
          Task.now(Some(ok.body.content.to[WalletTransaction]))

        case Failure(nf : NotFound[_]) ⇒
          Task.now(None)

        case Failure(o) ⇒
          Task.raiseError(o)
      }
  }
//
//  protected def createWallet(walletId: String): Task[Wallet] = {
//
//  }

  protected def validateTransaction(implicit request: WalletTransactionPut): Task[Long] = {
    val i = "invalid-transaction-parameter"
    val t = request.body
    if (Option(t.walletId).forall(_.isEmpty) || t.walletId != request.walletId) {
      br(i, s"wallet_id has invalid value: ${t.walletId}")
    } else
    if (Option(t.transactionId).forall(_.isEmpty) || t.transactionId != request.transactionId) {
      br(i, s"transaction_id has invalid value: ${t.transactionId}")
    } else
    if (!Option(t.status).contains(WalletTransactionStatus.NEW)) {
      br(i, s"status should be skipped or 'new', provided: ${t.status}")
    } else {
      Task.now(t.amount)
    }
  }

  private def br(s: String, d: String)(implicit mcx: MessagingContext) =
    Task.raiseError(BadRequest(ErrorBody(s, Some(d))))


  protected def hyperStorageWalletPath(walletId: String): String = s"wallet-service/wallets/$walletId"

  protected def hyperStorageWalletTransactionsPath(walletId: String): String = s"wallet-service/wallets/$walletId/transactions~"

  protected def hyperStorageWalletTransactionPath(walletId: String, transactionId: String): String = s"wallet-service/wallets/$walletId/transactions~/$transactionId"

  override def stopService(controlBreak: Boolean, timeout: FiniteDuration): Future[Unit] = Future {
    handlers.foreach(_.cancel())
    logger.info(s"${getClass.getName} stopped")
  }
}

private [wallet] case class WalletWithRevision(wallet: Wallet, revision: Long)