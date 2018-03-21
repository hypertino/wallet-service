package com.hypertino.services.wallet

import java.math.MathContext

import com.hypertino.binders.value._
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{BadRequest, Conflict, Created, DynamicBody, ErrorBody, GatewayTimeout, Header, Headers, MessagingContext, NotFound, Ok, PreconditionFailed, Response}
import com.hypertino.hyperbus.serialization.SerializationOptions
import com.hypertino.hyperbus.subscribe.Subscribable
import com.hypertino.hyperbus.util.ErrorUtils
import com.hypertino.service.control.api.Service
import com.hypertino.services.wallet.utils.ErrorCode
import com.hypertino.user.apiref.hyperstorage.{ContentGet, ContentPatch, ContentPut, HyperStorageHeader}
import com.hypertino.wallet.api._
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import scaldi.{Injectable, Injector}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

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
          val headers = Headers(
            ok.headers.filterKeys(key ⇒
              key.compareToIgnoreCase(Header.REVISION) == 0
              || key.compareToIgnoreCase(HyperStorageHeader.ETAG) == 0).toSeq :_*
          )
          Ok(ok.body.content.to[Wallet], headers=headers)

        case o ⇒ ErrorUtils.unexpected(o)
      }
  }

  def onWalletTransactionGet(implicit request: WalletTransactionGet): Task[Ok[WalletTransaction]] = {
    hyperbus
      .ask(ContentGet(hyperStorageWalletTransactionPath(request.walletId, request.transactionId)))
      .map {
        case ok@Ok(_: DynamicBody, _) ⇒
          val headers = Headers(
            ok.headers.filterKeys(key ⇒
              key.compareToIgnoreCase(Header.REVISION) == 0
                || key.compareToIgnoreCase(HyperStorageHeader.ETAG) == 0).toSeq :_*
          )
          Ok(ok.body.content.to[WalletTransaction], headers=headers)

        case o ⇒ ErrorUtils.unexpected(o)
      }
  }

  def onWalletTransactionPut(implicit request: WalletTransactionPut): Task[Response[Wallet]] = {
    validateTransaction(request).flatMap { amount ⇒
      saveTransaction(request.body).flatMap { alreadySaved ⇒
        if (alreadySaved) {
          onWalletGet(WalletGet(request.walletId))
        }
        else {
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
              case Failure(_: PreconditionFailed[_]) ⇒ Task.raiseError(GatewayTimeout(ErrorBody(ErrorCode.WALLET_UPDATE_FAILED,
                Some(s"Wallet update(s) conflict limit is reached ($LIMIT) for '${request.walletId}/${request.transactionId}'"))))

              case o ⇒ ErrorUtils.unexpectedTask(o)
            }
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

  protected def updateWallet(walletWithETag: WalletWithETag, amount: Long, transaction: WalletTransaction)
                            (implicit mcx: MessagingContext): Task[Response[Wallet]] = {
    val newAmount = walletWithETag.wallet.amount + amount
    val newWallet = walletWithETag.wallet.copy(
      amount=newAmount,
      minimum=transaction.minimum,
      maximum=transaction.maximum,
      lastTransactionId=transaction.transactionId,
      updatedAt=transaction.createdAt
    )

    checkBounds(newWallet).flatMap { _ ⇒
      val r = ContentPatch(hyperStorageWalletPath(transaction.walletId), DynamicBody(
        Obj.from("last_transaction_id" → transaction.transactionId, "amount" → newAmount)
      ), headers = Headers(HyperStorageHeader.IF_MATCH → walletWithETag.eTag))

      hyperbus
        .ask(r)
        .map { _ ⇒
          Ok(newWallet)
        }
    }
  }

  protected def createWallet(transaction: WalletTransaction)
                            (implicit mcx: MessagingContext): Task[Response[Wallet]] = {
    val newWallet = Wallet(
      walletId=transaction.walletId,
      amount=transaction.amount,
      minimum=transaction.minimum,
      maximum=transaction.maximum,
      lastTransactionId=transaction.transactionId,
      updatedAt=transaction.createdAt
    )

    checkBounds(newWallet).flatMap { _ ⇒
      val r = ContentPut(hyperStorageWalletPath(transaction.walletId), DynamicBody(
        newWallet.toValue
      ), headers = Headers(HyperStorageHeader.IF_NONE_MATCH → "*"))

      hyperbus
        .ask(r)
        .map { _ ⇒
          Created(newWallet)
        }
    }
  }

  private def checkBounds(wallet: Wallet)(implicit mc: MessagingContext): Task[Wallet] = {
    if (wallet.minimum.exists(wallet.amount < _))
      Task.raiseError(Conflict(ErrorBody(ErrorCode.WALLET_FALL_BELOW_MINIMUM, Some(s"${wallet.amount} is less than allowed ${wallet.minimum.get}"))))
    else
    if (wallet.maximum.exists(wallet.amount > _))
      Task.raiseError(Conflict(ErrorBody(ErrorCode.WALLET_MAXIMUM_EXCEEDED, Some(s"${wallet.amount} is greater than allowed ${wallet.maximum.get}"))))
    else
      Task.now(wallet)
  }

  protected def selectWallet(walletId: String)
                                    (implicit mcx: MessagingContext): Task[Option[WalletWithETag]] = {
    onWalletGet(WalletGet(walletId))
      .materialize
      .flatMap {
        case Success(ok @ Ok(w, _)) ⇒ Task.now(Some(WalletWithETag(w, ok.headers(HyperStorageHeader.ETAG).toString)))
        case Failure(NotFound(_, _)) ⇒ Task.now(None)
        case other ⇒ ErrorUtils.unexpectedTask(other)
      }
  }

  protected def applyLastTransaction(wallet: Wallet)
                                    (implicit mcx: MessagingContext): Task[Unit] = {

    selectTransaction(wallet.walletId, wallet.lastTransactionId).flatMap {
      case Some(transaction) ⇒
        if (transaction.status == WalletTransactionStatus.NEW)
          saveTransactionStatus(transaction, WalletTransactionStatus.APPLIED)
        else
          Task.unit

      case None ⇒
        Task.unit
    }
  }

  // returns true if the transaction is already saved, no need to update wallet amount
  protected def saveTransaction(transaction: WalletTransaction)
                               (implicit mcx: MessagingContext): Task[Boolean] = {
    hyperbus
      .ask(
        ContentPut(hyperStorageWalletTransactionPath(transaction.walletId, transaction.transactionId), DynamicBody(
        transaction.toValue
        ), headers = Headers(HyperStorageHeader.IF_NONE_MATCH → "*"))
      )
      .materialize
      .flatMap {
        case Success(_) ⇒
          Task.now(false)
        case Failure(_: PreconditionFailed[_]) ⇒
          validateExistingTransaction(transaction)
        case o ⇒
          ErrorUtils.unexpectedTask(o)
      }
  }

  protected def saveTransactionStatus(transaction: WalletTransaction, status: String)
                                     (implicit mcx: MessagingContext): Task[Unit] = {
    hyperbus
      .ask(ContentPatch(hyperStorageWalletTransactionPath(transaction.walletId, transaction.transactionId), DynamicBody(
        Obj.from("status" → status)
      ), headers = Headers(HyperStorageHeader.IF_MATCH → "*")))
      .map { _ ⇒ }
  }

  protected def selectTransaction(walletId: String, transactionId: String)
                                 (implicit mcx: MessagingContext): Task[Option[WalletTransaction]] = {
    hyperbus
      .ask(ContentGet(hyperStorageWalletTransactionPath(walletId, transactionId)))
      .materialize
      .flatMap {
        case Success(ok@Ok(_: DynamicBody, _)) ⇒
          Task.now(Some(ok.body.content.to[WalletTransaction]))

        case Failure(nf : NotFound[_]) ⇒
          Task.now(None)

        case o ⇒
          ErrorUtils.unexpectedTask(o)
      }
  }

  protected def validateTransaction(implicit request: WalletTransactionPut): Task[Long] = {
    val i = "invalid-transaction-parameter"
    val t = request.body
    if (Option(t.walletId).forall(_.isEmpty) || t.walletId != request.walletId) {
      br(i, s"wallet_id has invalid value: ${t.walletId}")
    } else if (Option(t.transactionId).forall(_.isEmpty) || t.transactionId != request.transactionId) {
      br(i, s"transaction_id has invalid value: ${t.transactionId}")
    } else if (!Option(t.status).contains(WalletTransactionStatus.NEW)) {
      br(i, s"status should be skipped or 'new', provided: ${t.status}")
    } else {
      Task.now(t.amount)
    }
  }

  protected def validateExistingTransaction(newTr: WalletTransaction)
                                           (implicit mcx: MessagingContext): Task[Boolean] = {
    selectTransaction(newTr.walletId, newTr.transactionId)
      .map { existingTrO ⇒
        existingTrO.map { e ⇒
          // if existing become applied it's fine even new still in status NEW
          val ecmp = if (e.status == WalletTransactionStatus.APPLIED && newTr.status == WalletTransactionStatus.NEW) {
            e.copy(status = WalletTransactionStatus.NEW)
          } else {
            e
          }

          if (ecmp != newTr) {
            throw Conflict(ErrorBody(ErrorCode.WALLET_DUPLICATE_TRANSACTION, Some(s"Can't overwrite existing transaction: ${newTr.walletId}/${newTr.transactionId}")))
          }
          else {
            true
          }
        } getOrElse {
          false
        }
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

private [wallet] case class WalletWithETag(wallet: Wallet, eTag: String)