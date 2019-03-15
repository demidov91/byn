from unittest import mock

import pytest

from byn import retry



def test_retryable__once():
    real_method = mock.Mock(side_effect=[ValueError, 42])
    f = retry.retryable(real_method, ValueError)

    assert f(202, '03') == 42

    assert real_method.call_count == 2
    real_method.assert_called_with(202, '03')


def test_retryable__once_with_callback():
    callback_method = mock.Mock()
    real_method = mock.Mock(side_effect=[ValueError, 42])
    f = retry.retryable(real_method, ValueError, callback=callback_method)

    assert f(202, '03') == 42

    assert real_method.call_count == 2
    callback_method.assert_called_once_with(202, '03')
    real_method.assert_called_with(202, '03')


def test_retryable__twice_error():
    real_method = mock.Mock(side_effect=[ValueError, ValueError, 42])
    f = retry.retryable(real_method, ValueError)

    with pytest.raises(ValueError):
        f(202, '03')

    assert real_method.call_count == 2
    real_method.assert_called_with(202, '03')


def test_retryable__once_but_different():
    real_method = mock.Mock(side_effect=[NotImplementedError, 42])
    f = retry.retryable(real_method, ValueError)

    with pytest.raises(NotImplementedError):
        f(202, '03')

    real_method.assert_called_once_with(202, '03')


def test_retryable__twice_error_still_working():
    callback_method = mock.Mock()
    real_method = mock.Mock(side_effect=[ValueError, ValueError, 42])
    f = retry.retryable(real_method, ValueError, retry_count=2, callback=callback_method)

    assert f(202, '03') == 42

    assert real_method.call_count == 3
    assert callback_method.call_count == 2
    real_method.assert_called_with(202, '03')


def test_retryable__different_errors_still_working():
    real_method = mock.Mock(side_effect=[NotImplementedError, ValueError, 42])
    f = retry.retryable(real_method, (ValueError, NotImplementedError), retry_count=2)

    assert f(202, '03') == 42

    assert real_method.call_count == 3
    real_method.assert_called_with(202, '03')

